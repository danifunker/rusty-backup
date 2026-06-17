//! PARC Alto disk probe / validation tool.
//!
//! Imports period CopyDisk packs (`.bfs` / `.copydisk` / `.altodisk`),
//! round-trips them through the PARC Disk Image (PDI) container, and walks the
//! Basic File System (BFS) directory — exercising the whole read path against
//! real data.
//!
//! Usage:
//!   parc_probe <file-or-dir> [<file-or-dir> ...]
//!   parc_probe --write-pdi <out.pdi> <pack>
//!
//! A directory argument is scanned recursively for candidate packs.

use std::fs;
use std::path::{Path, PathBuf};

use std::collections::BTreeMap;

use rusty_backup::fs::alto::bfs::{Bfs, BfsFilesystem};
use rusty_backup::fs::alto::{open_pack, pdi, write, Disk, Geometry};
use rusty_backup::fs::filesystem::Filesystem;

/// Map of user-file name -> bytes (excludes SysDir / DiskDescriptor).
fn user_file_map(disk: &Disk) -> BTreeMap<String, Vec<u8>> {
    let bfs = Bfs::new(disk);
    bfs.list_files()
        .unwrap_or_default()
        .into_iter()
        .filter(|f| {
            let b = f.name.trim_end_matches('.');
            !b.eq_ignore_ascii_case("SysDir") && !b.eq_ignore_ascii_case("DiskDescriptor")
        })
        .map(|f| {
            let data = bfs
                .read_file_bytes(f.leader_vda, usize::MAX)
                .unwrap_or_default();
            (f.name.clone(), data)
        })
        .collect()
}

fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}

fn is_pack(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n.ends_with(".pdi")
        || n.ends_with(".dsk")
        || n.ends_with(".dsk44") // ContrAlto Diablo-44
        || n.ends_with(".dsk80") // ContrAlto Trident T-80
        || n.ends_with(".dsk300") // ContrAlto Trident T-300
        || n.contains(".bfs")
        || n.contains(".copydisk")
        || n.contains(".altodisk")
}

fn collect_packs(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        if let Ok(entries) = fs::read_dir(path) {
            let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
            paths.sort();
            for p in paths {
                collect_packs(&p, out);
            }
        }
    } else if path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(is_pack)
    {
        out.push(path.to_path_buf());
    }
}

/// Returns true on success.
fn probe_one(path: &Path) -> bool {
    let label = path.display();
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            println!("FAIL  {label}: read error: {e}");
            return false;
        }
    };

    let disk = match open_pack(&bytes) {
        Ok(d) => d,
        Err(e) => {
            println!("SKIP  {label}: {e}");
            return false;
        }
    };
    let g = &disk.geometry;
    println!(
        "PACK  {label}\n      model={} geom={}x{}x{}x{} sectors={} ({} bytes raw)",
        g.disk_model,
        g.n_disks,
        g.n_cylinders,
        g.n_heads,
        g.n_sectors,
        g.total_sectors(),
        bytes.len(),
    );

    // PDI round-trip: CopyDisk -> Disk -> PDI -> Disk', compare.
    let pdi_bytes = pdi::write(&disk);
    match pdi::read(&pdi_bytes) {
        Ok(back) => {
            let same = back.sectors.len() == disk.sectors.len()
                && back
                    .sectors
                    .iter()
                    .zip(&disk.sectors)
                    .all(|(a, b)| a.label == b.label && a.data == b.data);
            println!(
                "      PDI: {} bytes, round-trip {}",
                pdi_bytes.len(),
                if same { "OK" } else { "MISMATCH" }
            );
            if !same {
                return false;
            }
        }
        Err(e) => {
            println!("      PDI: write/read FAIL: {e}");
            return false;
        }
    }

    // SysDir leader name (label/leader offset sanity check).
    match Bfs::new(&disk).leader_name(rusty_backup::fs::alto::bfs::SYSDIR_LEADER_VDA) {
        Some(n) => println!("      SysDir leader name @VDA1: {n:?}"),
        None => println!("      SysDir leader name @VDA1: <none>"),
    }

    // BFS directory walk.
    let files = match Bfs::new(&disk).list_files() {
        Ok(f) => f,
        Err(e) => {
            println!("      BFS: walk FAIL: {e}");
            return false;
        }
    };
    let total_bytes: u64 = files.iter().map(|f| f.size).sum();
    println!(
        "      BFS: {} files, {total_bytes} bytes total",
        files.len()
    );
    for f in files.iter().take(12) {
        println!(
            "        {:<28} {:>8} bytes  {:>4} pages  ldr@{:<5} sn={}",
            f.name, f.size, f.n_pages, f.leader_vda, f.serial
        );
    }
    if files.len() > 12 {
        println!("        ... and {} more", files.len() - 12);
    }

    let mut ok = true;

    // DiskDescriptor: free-space + geometry cross-check. The DiskDescriptor is
    // located and read through the normal page-chain walk, so a geometry match
    // is an end-to-end proof that file extraction is byte-correct.
    match Bfs::new(&disk).disk_descriptor() {
        Ok(dd) => {
            let g = &disk.geometry;
            let geom_match = dd.n_disks == g.n_disks
                && dd.n_tracks == g.n_cylinders
                && dd.n_heads == g.n_heads
                && dd.n_sectors == g.n_sectors;
            let used = g.total_sectors() as u64 - dd.free_pages as u64;
            if geom_match {
                println!(
                    "      DiskDescriptor: {}/{} pages used, geometry cross-check OK",
                    used,
                    g.total_sectors()
                );
            } else {
                // Expected on partition captures: the DiskDescriptor records the
                // whole parent disk's shape, not this single partition's.
                println!(
                    "      DiskDescriptor: recorded geometry {}x{}x{}x{} != container \
                     {}x{}x{}x{} (partition image?)",
                    dd.n_disks,
                    dd.n_tracks,
                    dd.n_heads,
                    dd.n_sectors,
                    g.n_disks,
                    g.n_cylinders,
                    g.n_heads,
                    g.n_sectors
                );
            }
        }
        Err(e) => println!("      DiskDescriptor: {e}"),
    }

    // Exercise the Filesystem trait (root -> list_directory -> read_file).
    let first = files.first().map(|f| (f.name.clone(), f.size));
    let n_files = files.len();
    let mut tfs = BfsFilesystem::open(disk);
    let root = tfs.root().expect("root");
    match tfs.list_directory(&root) {
        Ok(entries) => {
            let list_match = entries.len() == n_files;
            println!(
                "      Trait: fs_type={:?} total={} used={} list={} {}",
                tfs.fs_type(),
                tfs.total_size(),
                tfs.used_size(),
                entries.len(),
                if list_match { "OK" } else { "MISMATCH" }
            );
            ok &= list_match;
            if let (Some((name, size)), Some(e0)) = (first, entries.first()) {
                match tfs.read_file(e0, usize::MAX) {
                    Ok(bytes) => {
                        let rd_ok = bytes.len() as u64 == size;
                        println!(
                            "      Trait read {name:?}: {} bytes {}",
                            bytes.len(),
                            if rd_ok { "OK" } else { "SIZE MISMATCH" }
                        );
                        ok &= rd_ok;
                    }
                    Err(e) => {
                        println!("      Trait read FAIL: {e}");
                        ok = false;
                    }
                }
            }
        }
        Err(e) => {
            println!("      Trait list_directory FAIL: {e}");
            ok = false;
        }
    }
    ok
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: parc_probe <file-or-dir> [...]");
        eprintln!("       parc_probe --write-pdi <out.pdi> <pack>");
        std::process::exit(2);
    }

    if args[0] == "--write-pdi" {
        if args.len() != 3 {
            eprintln!("usage: parc_probe --write-pdi <out.pdi> <pack>");
            std::process::exit(2);
        }
        let bytes = fs::read(&args[2]).expect("read pack");
        let disk = open_pack(&bytes).expect("open pack");
        fs::write(&args[1], pdi::write(&disk)).expect("write pdi");
        println!(
            "wrote {} ({} sectors)",
            args[1],
            disk.geometry.total_sectors()
        );
        return;
    }

    // Export a pack as a Salto cooked .dsk via our writer — to boot in Salto.
    //   --write-salto <out.dsk> <pack>     pure sector round-trip (preserves boot)
    //   --salto-rebuild <out.dsk> <pack>   defragmenting rebuild (build_disk)
    if args[0] == "--write-salto" || args[0] == "--salto-rebuild" {
        if args.len() != 3 {
            eprintln!("usage: parc_probe {} <out.dsk> <pack>", args[0]);
            std::process::exit(2);
        }
        let bytes = fs::read(&args[2]).expect("read pack");
        let src = open_pack(&bytes).expect("open pack");
        let disk = if args[0] == "--salto-rebuild" {
            write::clone_to(&src, src.geometry.clone()).expect("rebuild")
        } else {
            src
        };
        let dsk = rusty_backup::fs::alto::salto::write(&disk).expect("salto write");
        fs::write(&args[1], &dsk).expect("write dsk");
        println!(
            "wrote {} ({} bytes, {} sectors)",
            args[1],
            dsk.len(),
            disk.geometry.total_sectors()
        );
        return;
    }

    if args[0] == "bootchain" && args.len() == 2 {
        // Follow the chain the boot microcode follows: start at real DA 0, read
        // the page, follow its label `next` link, until eofDA/0. Report the page
        // count and a hash of the loaded image (the OutLd memory snapshot).
        let bytes = fs::read(&args[1]).expect("read");
        let disk = open_pack(&bytes).expect("open");
        let g = &disk.geometry;
        let mut vda = 0usize;
        let mut image: Vec<u8> = Vec::new();
        let mut pages = 0usize;
        let mut seen = std::collections::HashSet::new();
        loop {
            if !seen.insert(vda) {
                println!("  LOOP at vda {vda}");
                break;
            }
            let Some(s) = disk.sector(vda) else { break };
            let w = |i: usize| u16::from_be_bytes([s.label[i], s.label[i + 1]]);
            let (next, _prev, nc, page, _v, s1, s2) = (w(0), w(2), w(6), w(8), w(10), w(12), w(14));
            if pages < 3 {
                println!("  vda{vda}: page={page} numChars={nc} next={next:#06x}(vda {}) sn=({s1:#x},{s2:#x})", g.vda_from_da(next));
            }
            image.extend_from_slice(&s.data[..(nc as usize).min(s.data.len())]);
            pages += 1;
            if next == 0xffff || next == 0 || pages > g.total_sectors() {
                break;
            }
            vda = g.vda_from_da(next);
        }
        println!(
            "BOOTCHAIN {} : {pages} pages, {} bytes, sha {}",
            args[1],
            image.len(),
            sha256_hex(&image)
        );
        return;
    }

    if args[0] == "dbg" && args.len() == 3 {
        // dbg <pack> <name>: dump a file's leader + first chain pages.
        let bytes = fs::read(&args[1]).expect("read pack");
        let disk = open_pack(&bytes).expect("open");
        let want = args[2].trim_end_matches('.');
        let g = &disk.geometry;
        let lbl = |vda: usize| {
            let s = disk.sector(vda).unwrap();
            let w = |i: usize| u16::from_be_bytes([s.label[i], s.label[i + 1]]);
            (w(0), w(2), w(6), w(8), w(10), w(12), w(14)) // next prev numChars page ver sn1 sn2
        };
        for f in Bfs::new(&disk).list_files().expect("list") {
            if f.name.trim_end_matches('.').eq_ignore_ascii_case(want) {
                println!(
                    "FILE {} serial={:#x} v{} leader_vda={} size={} pages={}",
                    f.name, f.serial, f.version, f.leader_vda, f.size, f.n_pages
                );
                let (nx, pv, nc, pg, ver, s1, s2) = lbl(f.leader_vda);
                println!("  leader@{}: next={:#06x}(vda {}) prev={:#06x} numChars={} page={} ver={} sn=({:#x},{:#x})",
                    f.leader_vda, nx, g.vda_from_da(nx), pv, nc, pg, ver, s1, s2);
                let mut link = nx;
                for _ in 0..4 {
                    if link == 0xffff {
                        println!("  -> eofDA");
                        break;
                    }
                    let vda = g.vda_from_da(link);
                    let (nx, pv, nc, pg, _v, s1, s2) = lbl(vda);
                    println!("  page@vda{}: next={:#06x}(vda {}) prev={:#06x} numChars={} page={} sn=({:#x},{:#x})",
                        vda, nx, g.vda_from_da(nx), pv, nc, pg, s1, s2);
                    link = nx;
                }
            }
        }
        return;
    }

    if args[0] == "rebuild-check" {
        // Validate the writer against real data: rebuild every user file into a
        // fresh volume of the same geometry and confirm the file set + every
        // file's bytes come back identical.
        let mut packs = Vec::new();
        for a in &args[1..] {
            collect_packs(Path::new(a), &mut packs);
        }
        let mut ok = 0usize;
        for p in &packs {
            let bytes = match fs::read(p) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let src = match open_pack(&bytes) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let rebuilt = match write::clone_to(&src, src.geometry.clone()) {
                Ok(d) => d,
                Err(e) => {
                    println!("REBUILD-FAIL {}: {e}", p.display());
                    continue;
                }
            };
            let a = user_file_map(&src);
            let b = user_file_map(&rebuilt);
            if a == b {
                ok += 1;
                println!("OK   {} ({} user files)", p.display(), a.len());
            } else {
                println!(
                    "DIFF {} (src {} files, rebuilt {} files)",
                    p.display(),
                    a.len(),
                    b.len()
                );
                // Name the first few files that differ (missing, or size/content).
                let mut shown = 0;
                for (name, av) in &a {
                    match b.get(name) {
                        Some(bv) if bv == av => {}
                        Some(bv) => {
                            println!("     ~ {name}: src {} B, rebuilt {} B", av.len(), bv.len());
                            shown += 1;
                        }
                        None => {
                            println!("     - {name}: missing in rebuilt (src {} B)", av.len());
                            shown += 1;
                        }
                    }
                    if shown >= 5 {
                        break;
                    }
                }
                for name in b.keys() {
                    if !a.contains_key(name) {
                        println!("     + {name}: only in rebuilt");
                    }
                }
            }
        }
        println!("=== {ok}/{} packs rebuilt byte-identical ===", packs.len());
        return;
    }

    if args[0] == "resize" {
        // parc_probe resize <pack> <31|44> <out.pdi>
        if args.len() != 4 {
            eprintln!("usage: parc_probe resize <pack> <31|44> <out.pdi>");
            std::process::exit(2);
        }
        let src = open_pack(&fs::read(&args[1]).expect("read")).expect("open");
        let mut g: Geometry = src.geometry.clone();
        match args[2].as_str() {
            "31" => {
                g.disk_model = 31;
                g.n_cylinders = 203;
                g.n_heads = 2;
                g.n_sectors = 12;
            }
            "44" => {
                g.disk_model = 44;
                g.n_cylinders = 406;
                g.n_heads = 2;
                g.n_sectors = 12;
            }
            other => {
                eprintln!("unknown model {other} (use 31 or 44)");
                std::process::exit(2);
            }
        }
        let out = write::clone_to(&src, g).expect("resize");
        fs::write(&args[3], pdi::write(&out)).expect("write");
        println!(
            "resized -> {} ({} sectors, {} user files)",
            args[3],
            out.geometry.total_sectors(),
            user_file_map(&out).len()
        );
        return;
    }

    if args[0] == "add" || args[0] == "del" {
        // parc_probe add <pack> <name> <hostfile> <out.pdi>
        // parc_probe del <pack> <name> <out.pdi>
        let src = open_pack(&fs::read(&args[1]).expect("read")).expect("open");
        let (out_disk, out_path) = if args[0] == "add" {
            let data = fs::read(&args[3]).expect("read host file");
            (
                write::add_file(&src, &args[2], &data).expect("add"),
                &args[4],
            )
        } else {
            (write::delete_file(&src, &args[2]).expect("del"), &args[3])
        };
        fs::write(out_path, pdi::write(&out_disk)).expect("write");
        println!(
            "{} -> {} ({} user files)",
            args[0],
            out_path,
            user_file_map(&out_disk).len()
        );
        return;
    }

    if args[0] == "bitmap" {
        // Empirical check of the DiskDescriptor free-page bit table:
        // bit for VDA v is word (16 + v>>4), mask 0x8000 >> (v & 15);
        // set = in use. Confirm bitmap-free == KDH.freePages and dump a free
        // page's label fileId (the free-page marker).
        let bytes = fs::read(&args[1]).expect("read pack");
        let disk = open_pack(&bytes).expect("open pack");
        let dd = Bfs::new(&disk)
            .read_file_by_name("DiskDescriptor", 256 * 1024)
            .expect("read DiskDescriptor");
        let rd16 = |b: &[u8], i: usize| ((b[i] as u16) << 8) | b[i + 1] as u16;
        let total = disk.geometry.total_sectors();
        let bit_used = |v: usize| -> bool {
            let off = (16 + (v >> 4)) * 2;
            if off + 2 > dd.len() {
                return true;
            }
            (rd16(&dd, off) >> (15 - (v & 15))) & 1 == 1
        };
        let used = (0..total).filter(|&v| bit_used(v)).count();
        let kdh_free = rd16(&dd, 18) as usize;
        println!(
            "total={total} bitmap_used={used} bitmap_free={} KDH.freePages={kdh_free}  {}",
            total - used,
            if total - used == kdh_free {
                "MATCH"
            } else {
                "MISMATCH"
            }
        );
        if let Some(fv) = (2..total).find(|&v| !bit_used(v)) {
            let lab = &disk.sector(fv).unwrap().label;
            println!(
                "first free VDA {fv}: label fileId = {:04x} {:04x} {:04x} (version, sn.hi, sn.lo)",
                rd16(lab, 10),
                rd16(lab, 12),
                rd16(lab, 14)
            );
        }
        return;
    }

    if args[0] == "extract" {
        if args.len() < 3 {
            eprintln!("usage: parc_probe extract <pack> <name> [out]");
            std::process::exit(2);
        }
        let bytes = fs::read(&args[1]).expect("read pack");
        let disk = open_pack(&bytes).expect("open pack");
        let data = Bfs::new(&disk)
            .read_file_by_name(&args[2], usize::MAX)
            .expect("extract");
        if let Some(out) = args.get(3) {
            fs::write(out, &data).expect("write out");
            println!("wrote {} ({} bytes)", out, data.len());
        } else {
            println!(
                "{} = {} bytes  sha256={}",
                args[2],
                data.len(),
                sha256_hex(&data)
            );
        }
        return;
    }

    let mut packs = Vec::new();
    for a in &args {
        collect_packs(Path::new(a), &mut packs);
    }
    if packs.is_empty() {
        eprintln!("no .bfs/.copydisk/.altodisk packs found");
        std::process::exit(1);
    }

    let mut ok = 0usize;
    let total = packs.len();
    for p in &packs {
        if probe_one(p) {
            ok += 1;
        }
        println!();
    }
    println!("=== {ok}/{total} packs fully validated ===");
}
