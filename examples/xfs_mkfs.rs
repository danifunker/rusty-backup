//! Format a blank XFS image with our own creator, for cross-checking against
//! the real `xfs_repair` (see `scripts/xfs-oracle.sh`).
//!
//!   cargo run --example xfs_mkfs -- <image-path> <size-bytes> [label]

use std::fs::File;
use std::process::ExitCode;

use rusty_backup::fs::xfs::format::write_blank_xfs;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: xfs_mkfs <image-path> <size-bytes> [label]");
        return ExitCode::FAILURE;
    }
    let size: u64 = match args[2].parse() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("bad size {}: {e}", args[2]);
            return ExitCode::FAILURE;
        }
    };
    let label = args.get(3).map(String::as_str).unwrap_or("rbtest");

    let mut file = match File::create(&args[1]) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("creating {}: {e}", args[1]);
            return ExitCode::FAILURE;
        }
    };
    match write_blank_xfs(&mut file, 0, size, label) {
        Ok(len) => {
            if let Err(e) = file.set_len(len) {
                eprintln!("sizing {}: {e}", args[1]);
                return ExitCode::FAILURE;
            }
            println!("wrote {} ({len} bytes, label {label:?})", args[1]);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("formatting XFS: {e}");
            ExitCode::FAILURE
        }
    }
}
