//! Survey a PFS3 image for the things our v1 clone has to special-case:
//! hardlinks (LINKFILE/LINKDIR), softlinks (SOFTLINK), and the deldir
//! (PFS3 trashcan). Reports counts + a few example names per kind for
//! each PFS3 partition discovered via the RDB partition table.
//!
//! Usage: cargo run --release --example survey_pfs3_links -- <image>

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::pfs3::Pfs3Filesystem;
use rusty_backup::fs::{is_amiga_pfs3_type, Filesystem};
use rusty_backup::partition::rdb::Rdb;
use std::fs::File;
use std::io::BufReader;

fn walk(
    fs: &mut Pfs3Filesystem<BufReader<File>>,
    dir: &FileEntry,
    softlinks: &mut Vec<(String, String)>,
    hardlinks: &mut Vec<(String, u64)>,
    file_count: &mut usize,
    dir_count: &mut usize,
) {
    let kids = match fs.list_directory(dir) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("  list_directory({}) failed: {e}", dir.path);
            return;
        }
    };
    for k in kids {
        if k.is_symlink() {
            softlinks.push((k.path.clone(), k.symlink_target.clone().unwrap_or_default()));
        }
        if let Some(t) = k.link_target_cnid {
            hardlinks.push((k.path.clone(), t));
        }
        if k.is_directory() && k.link_target_cnid.is_none() {
            *dir_count += 1;
            walk(fs, &k, softlinks, hardlinks, file_count, dir_count);
        } else if k.is_file() && k.link_target_cnid.is_none() {
            *file_count += 1;
        }
    }
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/dani/amiga-filesystems/AmigaVision.hdf".to_string());
    let mut f = BufReader::new(File::open(&path).expect("open image"));
    let rdb = Rdb::parse(&mut f).expect("parse RDB");
    println!("Image: {path}");
    println!("Partitions: {}", rdb.partitions.len());
    for (i, p) in rdb.partitions.iter().enumerate() {
        let ts = p.dos_type_string();
        if !is_amiga_pfs3_type(&ts) {
            println!("  [{i}] {ts} - skipped (not PFS3)");
            continue;
        }
        println!(
            "  [{i}] {} ({}) start_lba={} size={}",
            p.drv_name,
            ts,
            p.start_lba_512(),
            p.size_bytes()
        );
        let f2 = BufReader::new(File::open(&path).expect("reopen"));
        let mut fs = match Pfs3Filesystem::open(f2, p.start_lba_512() * 512) {
            Ok(fs) => fs,
            Err(e) => {
                println!("    open failed: {e}");
                continue;
            }
        };
        let root = fs.root().expect("root");
        let mut softlinks = Vec::new();
        let mut hardlinks = Vec::new();
        let mut file_count = 0;
        let mut dir_count = 0;
        walk(
            &mut fs,
            &root,
            &mut softlinks,
            &mut hardlinks,
            &mut file_count,
            &mut dir_count,
        );
        println!("    files: {file_count}, directories: {dir_count}");
        println!("    softlinks: {}", softlinks.len());
        for (path, target) in softlinks.iter().take(5) {
            println!("      {} -> {}", path, target);
        }
        if softlinks.len() > 5 {
            println!("      ... and {} more", softlinks.len() - 5);
        }
        println!("    hardlinks: {}", hardlinks.len());
        for (path, target_anode) in hardlinks.iter().take(5) {
            println!("      {} -> anode {}", path, target_anode);
        }
        if hardlinks.len() > 5 {
            println!("      ... and {} more", hardlinks.len() - 5);
        }
        // Try to peek the deldir presence by checking rootblock options.
        // We can't read it without more API, but we can hint at it from
        // the option bits.
        println!("    (deldir contents not enumerated; pfs3aio MODE_DELDIR bit)");
    }
}
