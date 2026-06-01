use rusty_backup::fs::filesystem::Filesystem;

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: test_ntfs_gho <path>");
    let path = std::path::Path::new(&path);

    let reader = rusty_backup::rbformats::gho::GhoReader::open(path).unwrap();
    println!("Runs: {}", reader.block_count());

    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(reader, 0).unwrap();
    println!("Volume: {:?}", fs.volume_label());

    let root = fs.root().unwrap();
    println!("Root: location={}", root.location);

    match fs.list_directory(&root) {
        Ok(children) => {
            println!("Root has {} entries:", children.len());
            for c in children.iter().take(30) {
                println!(
                    "  {} {:<40} {:>12} bytes",
                    if c.is_directory() { "DIR " } else { "FILE" },
                    c.name,
                    c.size
                );
            }
        }
        Err(e) => println!("list_directory error: {e}"),
    }
}
