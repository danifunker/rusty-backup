//! Stress-test the inobt new-chunk allocation path (§2.1 hole A): create
//! zero-byte regular files under the root of a v4 XFS image until either the
//! volume runs out of space or `count` files have been created. Used to
//! oracle-validate `alloc_new_inode_chunk` end-to-end via:
//!
//!   scripts/xfs-oracle.sh mkfs /tmp/t.img 16
//!   cargo run --example xfs_fill_chunks -- /tmp/t.img 200
//!   scripts/xfs-oracle.sh repair /tmp/t.img       # expect: clean
//!
//! A fresh 16 MiB v4 image typically lands `~60` free inode slots in the
//! initial chunk, so `count >= 80` exhausts it and pushes through at least
//! one new-chunk allocation.
//!
//!   cargo run --example xfs_fill_chunks -- <image> <count> [partition-offset-bytes]

use std::fs::OpenOptions;
use std::io::Cursor;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::xfs::XfsFilesystem;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(path), Some(count_s)) = (args.first().cloned(), args.get(1).cloned()) else {
        eprintln!("usage: xfs_fill_chunks <image> <count> [partition-offset-bytes]");
        return ExitCode::FAILURE;
    };
    let count: u64 = match count_s.parse() {
        Ok(n) if n > 0 => n,
        _ => {
            eprintln!("count must be a positive integer");
            return ExitCode::FAILURE;
        }
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

    let mut created = 0u64;
    for i in 0..count {
        let name = format!("fill{i:06}");
        let root = match fs.root() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("root after {created} creates: {e}");
                return ExitCode::FAILURE;
            }
        };
        let mut data = Cursor::new(Vec::<u8>::new());
        match fs.create_file(&root, &name, &mut data, 0, &CreateFileOptions::default()) {
            Ok(_) => created += 1,
            Err(e) => {
                eprintln!("stopped at file {i}: {e}");
                break;
            }
        }
    }
    if let Err(e) = fs.sync_metadata() {
        eprintln!("sync_metadata: {e}");
        return ExitCode::FAILURE;
    }
    println!("created {created}/{count} files");
    ExitCode::SUCCESS
}
