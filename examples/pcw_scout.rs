//! Scout: try every CP/M DPB preset against an EDSK file and report
//! which one parses cleanly. Used to identify the right DPB for new
//! TOSEC archives before anchoring a fixture.
//!
//! Usage: cargo run --release --example pcw_scout -- <file.dsk> [...]

use std::io::Cursor;

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::Dpb;
use rusty_backup::fs::cpm_diskdefs::ALL_PRESETS;
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::rbformats::containers::edsk::decode_edsk_bytes;

fn try_dpb(flat: &[u8], dpb: Dpb) -> Result<Vec<String>, String> {
    let cur = Cursor::new(flat.to_vec());
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, dpb).map_err(|e| format!("{e}"))?;
    let root = fs.root().map_err(|e| format!("{e}"))?;
    let entries = fs.list_directory(&root).map_err(|e| format!("{e}"))?;
    Ok(entries.iter().map(|e| e.name.clone()).collect())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: pcw_scout <file.dsk> [...]");
        std::process::exit(1);
    }
    for path in &args {
        println!("==== {path} ====");
        let raw = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                println!("  read error: {e}");
                continue;
            }
        };
        let flat = match decode_edsk_bytes(&raw) {
            Ok(f) => f,
            Err(e) => {
                println!("  EDSK decode error: {e}");
                continue;
            }
        };
        println!("  EDSK flat size: {}", flat.len());
        for dpb in ALL_PRESETS {
            match try_dpb(&flat, *dpb) {
                Ok(names) => {
                    let n = names.len();
                    let preview: Vec<&str> = names.iter().take(5).map(String::as_str).collect();
                    println!("  [{}] OK n={n} preview={preview:?}", dpb.name);
                }
                Err(e) => {
                    println!("  [{}] FAIL {e}", dpb.name);
                }
            }
        }
        println!();
    }
}
