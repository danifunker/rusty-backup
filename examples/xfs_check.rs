//! Run the XFS verifier against an image file and print the findings.
//!
//! Used as the in-repo side of the XFS repair oracle: create / corrupt a v4
//! image with the `rusty-xfs-oracle` Docker image (xfsprogs 4.9.0), then run
//! this to confirm our checker agrees with `xfs_repair -n`.
//!
//!   cargo run --example xfs_check -- <image-path> [partition-offset-bytes]
//!
//! Exit code is 0 when the verifier reports no errors, 1 otherwise.

use std::fs::File;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::xfs::XfsFilesystem;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: xfs_check <image-path> [partition-offset-bytes]");
        return ExitCode::FAILURE;
    };
    let offset: u64 = args.next().map(|s| s.parse().unwrap_or(0)).unwrap_or(0);

    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("cannot open {path}: {e}");
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

    let result = match fs.fsck() {
        Some(Ok(r)) => r,
        Some(Err(e)) => {
            eprintln!("fsck failed: {e}");
            return ExitCode::FAILURE;
        }
        None => {
            eprintln!("this filesystem does not implement fsck");
            return ExitCode::FAILURE;
        }
    };

    println!("=== stats ===");
    println!(
        "files checked: {}, directories checked: {}",
        result.stats.files_checked, result.stats.directories_checked
    );
    for (k, v) in &result.stats.extra {
        println!("  {k}: {v}");
    }

    if !result.warnings.is_empty() {
        println!("=== warnings ({}) ===", result.warnings.len());
        for w in &result.warnings {
            println!("  [{}] {}", w.code, w.message);
        }
    }

    if result.errors.is_empty() {
        println!("=== clean: no errors ===");
        ExitCode::SUCCESS
    } else {
        println!("=== errors ({}) ===", result.errors.len());
        for e in &result.errors {
            let tag = if e.repairable {
                "repairable"
            } else {
                "unrepairable"
            };
            println!("  [{}] ({tag}) {}", e.code, e.message);
        }
        if !result.orphaned_entries.is_empty() {
            println!(
                "=== orphaned inodes ({}) ===",
                result.orphaned_entries.len()
            );
            for o in &result.orphaned_entries {
                println!("  inode {} ({})", o.id, o.name);
            }
        }
        ExitCode::FAILURE
    }
}
