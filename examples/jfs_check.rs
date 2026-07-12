//! Run the JFS2 verifier (and, with `--repair`, the repair pass) against an
//! image file and print the findings.
//!
//! In-repo side of the JFS repair oracle: create a clean image with
//! `mkfs.jfs` (or `scripts/jfs-oracle.sh mkfs`), optionally forge an orphan,
//! then run this to confirm our checker agrees with the real `fsck.jfs`.
//!
//!   cargo run --example jfs_check -- <image-path> [partition-offset-bytes]
//!   cargo run --example jfs_check -- <image-path> --repair [offset]
//!
//! `--repair` opens the image read-write, adopts every orphaned inode into
//! `/lost+found`, prints the repair report, and re-runs the verifier. Exit
//! code is 0 when the (post-repair) verifier reports no errors, 1 otherwise.

use std::fs::OpenOptions;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::{EditableFilesystem, Filesystem};
use rusty_backup::fs::jfs::JfsFilesystem;

fn print_result(result: &rusty_backup::fs::fsck::FsckResult) {
    for e in &result.errors {
        println!("ERROR  [{}] {}", e.code, e.message);
    }
    for w in &result.warnings {
        println!("WARN   [{}] {}", w.code, w.message);
    }
    for (k, v) in &result.stats.extra {
        println!("stat   {k} = {v}");
    }
    println!(
        "files={} dirs={} errors={} warnings={} repairable={}",
        result.stats.files_checked,
        result.stats.directories_checked,
        result.errors.len(),
        result.warnings.len(),
        result.repairable,
    );
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let repair = args.iter().any(|a| a == "--repair");
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let Some(path) = positional.first().map(|s| s.as_str()) else {
        eprintln!("usage: jfs_check <image-path> [--repair] [partition-offset-bytes]");
        return ExitCode::FAILURE;
    };
    let offset: u64 = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    let file = match OpenOptions::new().read(true).write(repair).open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("failed to open {path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut fs = match JfsFilesystem::open(file, offset) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("failed to open JFS at offset {offset}: {e}");
            return ExitCode::FAILURE;
        }
    };

    if repair {
        match fs.repair() {
            Ok(report) => {
                for f in &report.fixes_applied {
                    println!("FIXED  {f}");
                }
                for f in &report.fixes_failed {
                    println!("FAILED {f}");
                }
                println!(
                    "repair: {} applied, {} failed, {} unrepairable",
                    report.fixes_applied.len(),
                    report.fixes_failed.len(),
                    report.unrepairable_count
                );
            }
            Err(e) => {
                eprintln!("repair failed: {e}");
                return ExitCode::FAILURE;
            }
        }
        if let Err(e) = fs.sync_metadata() {
            eprintln!("sync failed: {e}");
            return ExitCode::FAILURE;
        }
    }

    let result = match fs.fsck() {
        Some(Ok(r)) => r,
        Some(Err(e)) => {
            eprintln!("check failed: {e}");
            return ExitCode::FAILURE;
        }
        None => {
            eprintln!("JFS does not implement fsck");
            return ExitCode::FAILURE;
        }
    };
    print_result(&result);

    if result.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
