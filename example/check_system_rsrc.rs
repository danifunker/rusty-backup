use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::File;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let f = File::open(&path).unwrap();
    let mut fs = HfsFilesystem::open(f, 96 * 512).unwrap();
    let root = fs.root().unwrap();
    let kids = fs.list_directory(&root).unwrap();
    let sysf = kids.iter().find(|k| k.name == "System Folder").unwrap();
    let inside = fs.list_directory(sysf).unwrap();
    let sys = inside.iter().find(|k| k.name == "System").unwrap();
    let mut sys_full = sys.clone();
    let rsrc_size = fs.resource_fork_size(&sys_full);
    sys_full.resource_fork_size = Some(rsrc_size);
    println!("System data_size={} rsrc_size={}", sys_full.size, rsrc_size);
    let mut buf: Vec<u8> = Vec::new();
    let n = fs.write_resource_fork_to(&sys_full, &mut buf).unwrap();
    println!(
        "Wrote {} bytes of resource fork. First 16: {:02x?}",
        n,
        &buf[..16.min(buf.len())]
    );
}
