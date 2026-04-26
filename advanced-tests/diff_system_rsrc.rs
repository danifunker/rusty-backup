use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::File;

fn read_system_rsrc(path: &str, part_off: u64) -> Vec<u8> {
    let f = File::open(path).unwrap();
    let mut fs = HfsFilesystem::open(f, part_off).unwrap();
    let root = fs.root().unwrap();
    let kids = fs.list_directory(&root).unwrap();
    let sysf = kids.iter().find(|k| k.name == "System Folder").unwrap();
    let inside = fs.list_directory(sysf).unwrap();
    let sys = inside.iter().find(|k| k.name == "System").unwrap();
    let mut sys_full = sys.clone();
    sys_full.resource_fork_size = Some(fs.resource_fork_size(&sys_full));
    let mut buf: Vec<u8> = Vec::new();
    fs.write_resource_fork_to(&sys_full, &mut buf).unwrap();
    buf
}

fn main() {
    let mut args = std::env::args().skip(1);
    let src = args.next().unwrap();
    let src_off: u64 = args.next().unwrap().parse().unwrap();
    let tgt = args.next().unwrap();
    let tgt_off: u64 = args.next().unwrap().parse().unwrap();

    let s = read_system_rsrc(&src, src_off);
    let t = read_system_rsrc(&tgt, tgt_off);
    println!("source rsrc len: {}, target rsrc len: {}", s.len(), t.len());
    if s.len() != t.len() {
        println!("LENGTH MISMATCH!");
    }
    let cmp_len = s.len().min(t.len());
    let mut diffs = 0u64;
    let mut first_diff: Option<usize> = None;
    for i in 0..cmp_len {
        if s[i] != t[i] {
            diffs += 1;
            if first_diff.is_none() {
                first_diff = Some(i);
            }
        }
    }
    println!("byte diffs in common prefix: {}", diffs);
    if let Some(d) = first_diff {
        println!(
            "first diff at offset {} (0x{:x}): src=0x{:02x} tgt=0x{:02x}",
            d, d, s[d], t[d]
        );
    }
}
