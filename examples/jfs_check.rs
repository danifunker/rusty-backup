//! Run the JFS2 verifier against an image file and print the findings.
//!
//! In-repo side of the JFS repair oracle: create a clean image with the
//! `rusty-jfs-oracle` Docker image (jfsutils), optionally forge an orphan,
//! then run this to confirm our checker agrees with the real `fsck.jfs`.
//!
//!   cargo run --example jfs_check -- <image-path> [partition-offset-bytes]
//!
//! Exit code is 0 when the verifier reports no errors, 1 otherwise.
//!
//! (`--repair` will be added here once JFS grows an `EditableFilesystem`
//! repair path; today the example is check-only so the oracle can validate
//! our reader against `fsck.jfs` on clean images first.)

use std::fs::OpenOptions;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::jfs::JfsFilesystem;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(path) = args.first().cloned() else {
        eprintln!("usage: jfs_check <image-path> [partition-offset-bytes]");
        return ExitCode::FAILURE;
    };
    let offset: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    let file = match OpenOptions::new().read(true).open(&path) {
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

    if result.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
