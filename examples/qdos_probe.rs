//! Scout / diagnostic for QDOS QXL.WIN files. Run against a path; prints
//! header summary, root directory listing, and a hex preview of the first
//! small file. Used to scout-validate against real-world public samples
//! (e.g. kilgus QXL.WIN, smsqe.win) per the synthetic-fixture policy.

use std::io::Cursor;

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::qdos::QdosFilesystem;

fn main() {
    for path in std::env::args().skip(1) {
        println!("==== {path} ====");
        let bytes = std::fs::read(&path).unwrap();
        println!("  size: {} bytes", bytes.len());
        let cur = Cursor::new(bytes);
        match QdosFilesystem::open(cur, 0) {
            Ok(mut fs) => {
                println!(
                    "  open OK; type={} label={:?}",
                    fs.fs_type(),
                    fs.volume_label()
                );
                match fs.root().and_then(|r| fs.list_directory(&r)) {
                    Ok(entries) => {
                        println!("  root: {} entries", entries.len());
                        for e in entries.iter().take(10) {
                            println!("    - {} ({} B)", e.name, e.size);
                        }
                        if let Some(file) = entries.iter().find(|e| e.size > 0 && e.size < 4096) {
                            match fs.read_file(file, 64) {
                                Ok(bytes) => println!(
                                    "  first 64 B of {}: {:02X?}",
                                    file.name,
                                    &bytes[..bytes.len().min(64)]
                                ),
                                Err(e) => println!("  read error on {}: {e}", file.name),
                            }
                        }
                    }
                    Err(e) => println!("  list error: {e}"),
                }
            }
            Err(e) => println!("  open error: {e}"),
        }
    }
}
