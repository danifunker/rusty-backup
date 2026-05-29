//! Export a GHO to a whole-disk raw .img via the real export pipeline (MBR
//! reassembly + NTFS metadata synthesis). Used to validate that the result
//! mounts in a strict NTFS driver.
//!
//! usage: gho_export_raw <src.gho> <dst.img>

use rusty_backup::rbformats::export::{export_whole_disk, ExportFormat};
use std::path::Path;

fn main() {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let src = Path::new(&args[1]);
    let dst = Path::new(&args[2]);
    let mut last = 0u64;
    export_whole_disk(
        ExportFormat::Raw,
        src,
        None,
        None,
        &[],
        dst,
        |b| {
            // Throttle progress prints to each ~1 GB.
            if b / 1_000_000_000 != last / 1_000_000_000 {
                last = b;
                eprintln!("  {:.1} GB", b as f64 / 1e9);
            }
        },
        || false,
        |m| println!("{m}"),
    )
    .unwrap();
    println!("done: {}", dst.display());
}
