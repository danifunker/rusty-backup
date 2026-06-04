//! Scout / diagnostic for ADFS .hdf files. Opens via our existing reader,
//! reports header summary + root listing. Used to scout-validate against
//! real-world public samples (marutan.net blank256E.hdf etc.) per the
//! synthetic-fixture policy.

use std::io::Cursor;

use rusty_backup::fs::adfs::AdfsFilesystem;
use rusty_backup::fs::filesystem::Filesystem;

fn main() {
    for path in std::env::args().skip(1) {
        println!("==== {path} ====");
        let bytes = std::fs::read(&path).unwrap();
        println!("  size: {} bytes", bytes.len());
        let cur = Cursor::new(bytes);
        match AdfsFilesystem::open(cur, 0) {
            Ok(mut fs) => {
                println!(
                    "  open OK; type={} label={:?}",
                    fs.fs_type(),
                    fs.volume_label()
                );
                println!(
                    "  total_size={}, used_size={}",
                    fs.total_size(),
                    fs.used_size()
                );
                match fs.root().and_then(|r| fs.list_directory(&r)) {
                    Ok(entries) => {
                        println!("  root: {} entries", entries.len());
                        for e in entries.iter().take(10) {
                            println!("    - {} ({} B)", e.name, e.size);
                        }
                    }
                    Err(e) => println!("  list error: {e}"),
                }
            }
            Err(e) => println!("  open error: {e}"),
        }
    }
}
