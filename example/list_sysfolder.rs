use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::File;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let f = File::open(&path).unwrap();
    let mut fs = HfsFilesystem::open(f, 96 * 512).unwrap();
    let root = fs.root().unwrap();
    let kids = fs.list_directory(&root).unwrap();
    let sysf = kids
        .iter()
        .find(|k| k.name == "System Folder")
        .expect("no System Folder");
    println!("System Folder CNID = {}", sysf.location);
    let inside = fs.list_directory(sysf).unwrap();
    println!("{} entries inside System Folder:", inside.len());
    for k in inside.iter().take(40) {
        let kind = if matches!(k.entry_type, rusty_backup::fs::entry::EntryType::Directory) {
            "DIR "
        } else {
            "FILE"
        };
        println!(
            "  CNID {:5} [{}] {:?} type={:?} creator={:?} size={}",
            k.location, kind, k.name, k.type_code, k.creator_code, k.size
        );
    }
    if let Some(sys) = inside.iter().find(|k| k.name == "System") {
        println!(
            "\nSystem file: CNID {} type={:?} size={} (read attempt below)",
            sys.location, sys.type_code, sys.size
        );
        // no buf
        match fs.read_file(sys, 4096) {
            Ok(buf) => println!(
                "  read OK: {} bytes (first 16: {:02x?})",
                buf.len(),
                &buf[..16.min(buf.len())]
            ),
            Err(e) => println!("  read FAILED: {e:?}"),
        }
    } else {
        println!("\n!!! NO 'System' file found in System Folder — this would cause classic Mac OS error -127 !!!");
    }
}
