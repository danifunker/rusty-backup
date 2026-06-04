//! Scout: decode a `.d88` and report what's inside.

use std::io::{Cursor, Read};

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::rbformats::containers::d88;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: d88_scout <file.d88|file.d88.zst>");
        std::process::exit(1);
    }
    for path in &args {
        println!("==== {path} ====");
        let raw = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let raw = if path.ends_with(".zst") {
            let mut dec = zstd::stream::read::Decoder::new(Cursor::new(raw)).unwrap();
            let mut out = Vec::new();
            dec.read_to_end(&mut out).unwrap();
            out
        } else {
            raw
        };
        println!("  size: {} bytes", raw.len());
        match d88::decode_d88_bytes(&raw) {
            Ok(flat) => {
                println!("  decoded flat: {} bytes", flat.len());
                println!(
                    "  first 32 bytes of flat: {:02X?}",
                    &flat[..32.min(flat.len())]
                );
                // Try opening as Human68k.
                let cur = Cursor::new(flat);
                match Human68kFilesystem::open(cur, 0) {
                    Ok(mut fs) => {
                        println!("  Human68k: open OK");
                        match fs.root().and_then(|r| fs.list_directory(&r)) {
                            Ok(entries) => {
                                println!("  Human68k root: {} entries", entries.len());
                                for e in entries.iter().take(10) {
                                    println!("    - {} ({} B)", e.name, e.size);
                                }
                            }
                            Err(e) => println!("  Human68k list error: {e}"),
                        }
                    }
                    Err(e) => println!("  Human68k open error: {e}"),
                }
            }
            Err(e) => println!("  ERROR: {e}"),
        }
    }
}
