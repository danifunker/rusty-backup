use rusty_backup::fs::filesystem::Filesystem;
use std::io::{Read, Seek, SeekFrom};

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: test_ntfs_gho <path>");
    let path = std::path::Path::new(&path);

    let mut reader = rusty_backup::rbformats::gho::GhoReader::open(path).unwrap();
    println!("Runs: {}", reader.block_count());

    // Check if root dir index cluster (LCN 1310307) is readable
    let root_idx_off = 1310307u64 * 4096;
    reader.seek(SeekFrom::Start(root_idx_off)).unwrap();
    let mut buf = [0u8; 512];
    let n = reader.read(&mut buf).unwrap();
    let nz = buf.iter().filter(|&&b| b != 0).count();
    println!(
        "Root dir $INDEX_ALLOC (LCN 1310307): read {} bytes, {} non-zero",
        n, nz
    );
    if n >= 4 {
        println!("  First 4: {:02x?} (INDX = {:02x?})", &buf[0..4], b"INDX");
    }

    // Try NTFS browsing
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(&mut reader, 0).unwrap();
    let root = fs.root().unwrap();
    match fs.list_directory(&root) {
        Ok(children) => {
            println!("Root has {} entries:", children.len());
            for c in &children {
                println!(
                    "  {} {} {:>12} bytes",
                    if c.is_directory() { "DIR " } else { "FILE" },
                    c.name,
                    c.size
                );
            }
        }
        Err(e) => println!("list_directory error: {e}"),
    }
}
