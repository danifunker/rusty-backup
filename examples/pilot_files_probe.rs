//! Investigate the file population of a real Pilot volume (label scan).
//!
//! Usage: `pilot_files_probe <file.zdisk|file.pdi>`
//!
//! Groups every logical page by its label `fileID`, classifying by the word-7
//! `File.Type`/attribute, so we can see how many files a real ViewPoint/XDE
//! volume has, their page counts, and where the volume-structure files (VAM=7,
//! VFM=8) live — the empirical basis for classic-Pilot file enumeration.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rusty_backup::fs::alto::pilot::{self, Generation, Label, PilotFilesystem};
use rusty_backup::fs::alto::{open_pack, Disk};
use rusty_backup::fs::filesystem::Filesystem;

fn classic_file_page(l: &Label) -> u32 {
    // Classic Pilot label: filePageLo (w5) + filePageHi (w6 bits 0..6); the
    // high bits of w6 are page-0 flags. Label::parse already gives file_id; we
    // recompute the page number masking the flag bits.
    let lo = l.file_page & 0xffff;
    let hi = (l.file_page >> 16) & 0x7f;
    (hi << 16) | lo
}

fn type_name(t: u16) -> &'static str {
    match t {
        0 => "unassigned",
        1 => "physRoot",
        2 => "badPageList",
        3 => "badPage",
        4 => "subVolMarker",
        5 => "logRoot",
        6 => "freePage",
        7 => "VAM",
        8 => "VFM",
        9 => "scavengerLog",
        10 => "tempFileList",
        12 => "vmBackingFile",
        15 => "anonymousFile",
        9728 => "cedar:free",
        9729 => "cedar:header",
        9730 => "cedar:data",
        _ => "client/other",
    }
}

struct FileInfo {
    type_word: u16,
    pages: u32,
    min_page: u32,
    max_page: u32,
}

fn analyze_subvolume(disk: &Disk, pv_page: usize, n_pages: usize, label: &str) {
    let mut files: BTreeMap<[u16; 5], FileInfo> = BTreeMap::new();
    let mut free = 0usize;
    let mut structural = 0usize;
    for lp in 0..n_pages {
        let vda = pv_page + lp;
        let Some(s) = disk.sector(vda) else { continue };
        let l = Label::parse(&s.label);
        let t = l.attributes;
        if t == 6 || t == 9728 {
            free += 1;
            continue;
        }
        if matches!(t, 1..=5) {
            structural += 1;
            continue;
        }
        let fp = classic_file_page(&l);
        let e = files.entry(l.file_id).or_insert(FileInfo {
            type_word: t,
            pages: 0,
            min_page: u32::MAX,
            max_page: 0,
        });
        e.pages += 1;
        e.min_page = e.min_page.min(fp);
        e.max_page = e.max_page.max(fp);
    }

    println!(
        "\n  subvolume '{label}' @pv_page={pv_page} n_pages={n_pages}: {} files, {free} free, {structural} structural",
        files.len()
    );
    // Type histogram across files.
    let mut type_hist: BTreeMap<u16, usize> = BTreeMap::new();
    for f in files.values() {
        *type_hist.entry(f.type_word).or_default() += 1;
    }
    print!("    file-type histogram:");
    for (t, c) in &type_hist {
        print!(" {}={c}", type_name(*t));
    }
    println!();
    // Show the largest 12 files.
    let mut v: Vec<_> = files.iter().collect();
    v.sort_by_key(|(_, f)| std::cmp::Reverse(f.pages));
    println!("    largest files (fileID / type / pages / page-range):");
    for (id, f) in v.iter().take(12) {
        println!(
            "      [{:04x} {:04x} {:04x} {:04x} {:04x}] {:14} {:5} pages  [{}..{}]",
            id[0],
            id[1],
            id[2],
            id[3],
            id[4],
            type_name(f.type_word),
            f.pages,
            f.min_page,
            f.max_page
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(arg) = args.first() else {
        eprintln!("usage: pilot_files_probe <file.zdisk|file.pdi>");
        std::process::exit(2);
    };
    let bytes = fs::read(Path::new(arg)).expect("read");
    let disk = open_pack(&bytes).expect("open_pack");
    let vol = pilot::read_volume(&disk, Generation::OriginalPilot).expect("read_volume");
    println!(
        "PV '{}' : {} subvolumes",
        vol.pv_label,
        vol.physical_root.sub_volumes.len()
    );
    for (i, sv) in vol.physical_root.sub_volumes.iter().enumerate() {
        analyze_subvolume(
            &disk,
            sv.pv_page as usize,
            sv.n_pages as usize,
            &format!("sv{i}"),
        );
    }

    // Exercise the actual Filesystem-trait path the GUI/CLI use: list + read.
    println!("\n  === Filesystem trait (list + extract) ===");
    let mut fs = PilotFilesystem::open(disk, Generation::OriginalPilot).expect("open fs");
    let root = fs.root().expect("root");
    let entries = fs.list_directory(&root).expect("list");
    let named = entries.iter().filter(|e| !e.name.starts_with("LV")).count();
    println!(
        "  total files listed: {} ({named} with leader names, {} synthetic)",
        entries.len(),
        entries.len() - named
    );
    let mut by_size: Vec<_> = entries.iter().collect();
    by_size.sort_by_key(|e| std::cmp::Reverse(e.size));
    for e in by_size.iter().take(8) {
        // Read the whole file and checksum it, proving extraction works.
        let data = fs.read_file(e, usize::MAX).expect("read");
        let sum: u32 = data
            .iter()
            .fold(0u32, |a, &b| a.wrapping_mul(31).wrapping_add(b as u32));
        println!(
            "    {:28} size={:8} read={:8} cksum={:08x}",
            e.name,
            e.size,
            data.len(),
            sum
        );
    }
}
