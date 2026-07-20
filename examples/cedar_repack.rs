//! Repack a Cedar PDI authored by the historical one-header-page writer into
//! a Cedar-valid volume while preserving as many user files and names as fit.
//!
//! Usage:
//!   cedar_repack <old.pdi> <germ> <boot> <new.pdi> [max-files]

use std::fs;

use rusty_backup::fs::alto::pdi;
use rusty_backup::fs::alto::pilot::{self, Generation, PilotFilesystem, PvBootFile};
use rusty_backup::fs::filesystem::Filesystem;

const VOLUME_PAGES: u16 = 65535;

fn is_old_boot_payload(name: &str) -> bool {
    name.starts_with("LV0_00020000_") || name.starts_with("LV0_00030000_")
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 && args.len() != 5 {
        eprintln!("usage: cedar_repack <old.pdi> <germ> <boot> <new.pdi> [max-files]");
        std::process::exit(2);
    }
    let (old_path, germ_path, boot_path, new_path) = (&args[0], &args[1], &args[2], &args[3]);
    let max_files = args.get(4).map(|s| {
        s.parse::<usize>()
            .expect("max-files must be a non-negative integer")
    });

    let old_bytes = fs::read(old_path).expect("read old PDI");
    let old_disk = pdi::read(&old_bytes).expect("parse old PDI");
    let mut old_fs =
        PilotFilesystem::open(old_disk, Generation::CedarNucleus).expect("open old Cedar volume");
    let root = old_fs.root().expect("old root");
    let entries = old_fs.list_directory(&root).expect("list old volume");

    let germ = fs::read(germ_path).expect("read germ");
    let boot = fs::read(boot_path).expect("read boot file");
    let mut disk = pilot::create_blank(
        pilot::pilot_geometry(VOLUME_PAGES),
        Generation::CedarNucleus,
        "CedarKitchenSink",
    )
    .expect("create volume");
    disk = pilot::install_boot_file(&disk, Generation::CedarNucleus, PvBootFile::Germ, &germ)
        .expect("install germ");
    disk = pilot::install_boot_file(&disk, Generation::CedarNucleus, PvBootFile::BootFile, &boot)
        .expect("install boot file");
    // A one-leaf client B-tree is 4096 bytes: two normal header pages plus
    // eight data pages. Reserve that space before user files consume it.
    let (reserved_disk, client_reserve_fid) =
        pilot::add_file(&disk, Generation::CedarNucleus, &[0; 4096])
            .expect("reserve client directory space");
    disk = reserved_disk;

    let mut copied = 0usize;
    let mut skipped = 0usize;
    let mut names: Vec<(String, u16, u32)> = Vec::new();
    for entry in entries {
        if max_files.is_some_and(|limit| copied >= limit) {
            skipped += 1;
            continue;
        }
        if is_old_boot_payload(&entry.name) {
            continue;
        }
        let data = old_fs
            .read_file(&entry, usize::MAX)
            .unwrap_or_else(|e| panic!("read {}: {e}", entry.name));
        match pilot::add_file(&disk, Generation::CedarNucleus, &data) {
            Ok((next, fid)) => {
                disk = next;
                copied += 1;
                if !entry.name.starts_with("LV0_") {
                    names.push((entry.name.clone(), 1, fid));
                }
            }
            Err(_) => skipped += 1,
        }
        if (copied + skipped).is_multiple_of(100) {
            println!(
                "processed {} files: copied {copied}, skipped {skipped}",
                copied + skipped
            );
        }
    }

    // The old single-leaf client directory contains only a small named subset.
    // Trim from the end if the corrected file IDs make an entry set overflow.
    disk = pilot::delete_file(&disk, client_reserve_fid).expect("release client reservation");
    loop {
        match pilot::set_client_directory(&disk, Generation::CedarNucleus, &names) {
            Ok(next) => {
                disk = next;
                break;
            }
            Err(_) if !names.is_empty() => {
                names.pop();
            }
            Err(e) => panic!("install client directory: {e}"),
        }
    }

    // Report what the volume will actually show. set_client_directory drops
    // entries whose file it cannot resolve to a header page (writing a null
    // File.FP.da instead would make Cedar abandon the whole enumeration with
    // $badFP), so the number of names offered is not the number that land.
    {
        let mut fs = PilotFilesystem::open(disk.clone(), Generation::CedarNucleus)
            .expect("reopen new volume");
        let root = fs.root().expect("new root");
        let listed = fs.list_directory(&root).expect("list new volume");
        let real = listed
            .iter()
            .filter(|e| !e.name.starts_with("LV0_"))
            .count();
        println!(
            "directory: offered {} names, {real} resolved and listed{}",
            names.len(),
            if real < names.len() {
                format!(
                    " ({} dropped: no resolvable header page)",
                    names.len() - real
                )
            } else {
                String::new()
            }
        );
    }

    let out = pilot::write_pdi(&disk, Generation::CedarNucleus);
    fs::write(new_path, out).expect("write new PDI");
    println!(
        "wrote {new_path}: copied {copied}, skipped {skipped}, named {}",
        names.len()
    );
}
