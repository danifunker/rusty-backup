//! Pilot / Cedar volume probe + blank-volume creator.
//!
//! Exercises the Pilot/Cedar read + create path (PDI `fsFamily = 2`) defined in
//! `~/docs/PARC_PILOT_FORMAT.md`. There is no period Pilot disk to validate
//! against by design, so the bar is self-consistency: a created blank volume
//! reads back with verified root-page checksums and a consistent subvolume +
//! free-page count.
//!
//! Usage:
//!   pilot_probe new <pages> <cedar|pilot> <name> <out.pdi>
//!   pilot_probe probe <file.pdi>
//!   pilot_probe roundtrip <pages> <cedar|pilot> <name>   (create -> read, no file)

use std::fs;

use rusty_backup::fs::alto::pilot::{self, Generation, PilotFilesystem, PvBootFile};
use rusty_backup::fs::alto::{open_pack, pdi};
use rusty_backup::fs::filesystem::Filesystem;

fn parse_gen(s: &str) -> Generation {
    match s.to_ascii_lowercase().as_str() {
        "pilot" | "original" | "80" => Generation::OriginalPilot,
        _ => Generation::CedarNucleus,
    }
}

/// Recover the file-ID generation from a PDI's `flags` bit 2 (default Cedar).
fn pdi_generation(bytes: &[u8]) -> Generation {
    pdi::read_header(bytes)
        .map(|h| Generation::from_pdi_flag_bit2(h.flags & pdi::FLAG_PILOT_80BIT != 0))
        .unwrap_or(Generation::CedarNucleus)
}

fn print_volume(v: &pilot::PilotVolume) {
    let gen = match v.generation {
        Generation::CedarNucleus => "Cedar nucleus (32-bit FileID)",
        Generation::OriginalPilot => "original Pilot (80-bit UniversalID)",
    };
    println!("  generation : {gen}");
    println!("  PV label   : {:?}", v.pv_label);
    println!("  LV label   : {:?}  type={}", v.lv_label, v.volume_type);
    println!("  volume size: {} pages", v.volume_size);
    println!("  free pages : {} (from page labels)", v.free_pages);
    match v.vam_free_pages {
        Some(vf) if vf == v.free_pages => println!("  VAM        : {vf} free (agrees with labels)"),
        Some(vf) => println!(
            "  VAM        : {vf} free (DISAGREES with labels {})",
            v.free_pages
        ),
        None => println!("  VAM        : (none)"),
    }
    println!("  subvolumes : {}", v.physical_root.sub_volumes.len());
    for (i, sv) in v.physical_root.sub_volumes.iter().enumerate() {
        println!(
            "    [{i}] lvPage={} pvPage={} nPages={} lvSize={}",
            sv.lv_page, sv.pv_page, sv.n_pages, sv.lv_size
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("new") if args.len() == 5 => {
            let pages: u16 = args[1].parse().expect("pages");
            let gen = parse_gen(&args[2]);
            let name = &args[3];
            let out = &args[4];
            let disk =
                pilot::create_blank(pilot::pilot_geometry(pages), gen, name).expect("create_blank");
            let bytes = pilot::write_pdi(&disk, gen);
            fs::write(out, &bytes).expect("write pdi");
            println!("Wrote {out} ({} bytes)", bytes.len());
            let back = open_pack(&bytes).expect("reopen");
            let vol = pilot::read_volume(&back, gen).expect("read_volume");
            print_volume(&vol);
            println!("OK: created blank Pilot volume reads back consistently.");
        }
        Some("probe") if args.len() == 2 => {
            let bytes = fs::read(&args[1]).expect("read file");
            let disk = open_pack(&bytes).expect("open_pack");
            let gen = pdi_generation(&bytes);
            match pilot::read_volume(&disk, gen) {
                Ok(vol) => {
                    println!("PILOT  {}", args[1]);
                    print_volume(&vol);
                    let mut fsv = PilotFilesystem::open(disk, gen).expect("open fs");
                    let root = fsv.root().expect("root");
                    let files = fsv.list_directory(&root).expect("list");
                    println!("  files      : {}", files.len());
                    for f in &files {
                        println!(
                            "    {:<20} {:>8} bytes  @page {}",
                            f.name, f.size, f.location
                        );
                    }
                }
                Err(e) => println!("FAIL  {}: {e}", args[1]),
            }
        }
        Some("del") if args.len() == 3 => {
            // del <pdi> <fileID-decimal>
            let bytes = fs::read(&args[1]).expect("read pdi");
            let disk = open_pack(&bytes).expect("open_pack");
            let gen = pdi_generation(&bytes);
            let fid: u32 = args[2].parse().expect("FileID (decimal)");
            let disk = pilot::delete_file(&disk, fid).expect("delete_file");
            fs::write(&args[1], pilot::write_pdi(&disk, gen)).expect("write pdi");
            println!("Deleted FileID {fid} from {}", args[1]);
        }
        Some("add") if args.len() == 3 => {
            // add <pdi> <hostfile>  (rewrites the PDI in place with the file added)
            let bytes = fs::read(&args[1]).expect("read pdi");
            let disk = open_pack(&bytes).expect("open_pack");
            let gen = pdi_generation(&bytes);
            let payload = fs::read(&args[2]).expect("read host file");
            let (disk, fid) = pilot::add_file(&disk, gen, &payload).expect("add_file");
            fs::write(&args[1], pilot::write_pdi(&disk, gen)).expect("write pdi");
            println!(
                "Added {} ({} bytes) to {} as FileID {fid}",
                args[2],
                payload.len(),
                args[1]
            );
        }
        Some("install-boot") if args.len() == 4 => {
            // install-boot <pdi> <germ|bootfile|microcode> <hostfile>
            let slot = PvBootFile::parse(&args[2]).unwrap_or_else(|| {
                eprintln!("unknown boot slot {:?} (germ|bootfile|microcode)", args[2]);
                std::process::exit(2);
            });
            let bytes = fs::read(&args[1]).expect("read pdi");
            let disk = open_pack(&bytes).expect("open_pack");
            let gen = pdi_generation(&bytes);
            let payload = fs::read(&args[3]).expect("read host file");
            let disk =
                pilot::install_boot_file(&disk, gen, slot, &payload).expect("install_boot_file");
            fs::write(&args[1], pilot::write_pdi(&disk, gen)).expect("write pdi");
            // Verify the chain reads back byte-identical (page-padded).
            let back = open_pack(&fs::read(&args[1]).unwrap()).unwrap();
            let got = pilot::read_boot_file(&back, slot)
                .unwrap()
                .expect("present");
            let ok = got.len() >= payload.len() && got[..payload.len()] == payload[..];
            println!(
                "Installed {} boot file {} ({} bytes, {} pages) into {} -- chain verify {}",
                slot.label(),
                args[3],
                payload.len(),
                payload.len().div_ceil(512),
                args[1],
                if ok { "OK" } else { "FAILED" }
            );
        }
        Some("extract-boot") if args.len() == 4 => {
            // extract-boot <pdi> <germ|bootfile|microcode> <out>
            let slot = PvBootFile::parse(&args[2]).unwrap_or_else(|| {
                eprintln!("unknown boot slot {:?} (germ|bootfile|microcode)", args[2]);
                std::process::exit(2);
            });
            let bytes = fs::read(&args[1]).expect("read pdi");
            let disk = open_pack(&bytes).expect("open_pack");
            match pilot::read_boot_file(&disk, slot).expect("read_boot_file") {
                Some(data) => {
                    fs::write(&args[3], &data).expect("write out");
                    println!(
                        "Extracted {} boot file ({} pages, {} bytes) -> {}",
                        slot.label(),
                        data.len() / 512,
                        data.len(),
                        args[3]
                    );
                }
                None => {
                    eprintln!("slot {} is empty", slot.label());
                    std::process::exit(1);
                }
            }
        }
        Some("boot-info") if args.len() == 2 => {
            let bytes = fs::read(&args[1]).expect("read pdi");
            let disk = open_pack(&bytes).expect("open_pack");
            println!("BOOT  {}", args[1]);
            for slot in [
                PvBootFile::Microcode,
                PvBootFile::Germ,
                PvBootFile::BootFile,
                PvBootFile::Checkpoint,
            ] {
                let entry = pilot::boot_file_entry(&disk, slot).expect("boot_file_entry");
                if !entry.is_present() {
                    println!("  {:<11}: (empty)", slot.label());
                    continue;
                }
                match pilot::read_boot_file(&disk, slot) {
                    Ok(Some(data)) => println!(
                        "  {:<11}: fileID {:04X}{:04X} firstPage {} firstLink(VDA) {} -> {} pages, chain OK",
                        slot.label(),
                        entry.file_id[0],
                        entry.file_id[1],
                        entry.first_page,
                        entry.first_link,
                        data.len() / 512
                    ),
                    Ok(None) => println!("  {:<11}: (empty)", slot.label()),
                    Err(e) => println!("  {:<11}: present but chain INVALID: {e}", slot.label()),
                }
            }
        }
        Some("roundtrip") if args.len() == 4 => {
            let pages: u16 = args[1].parse().expect("pages");
            let gen = parse_gen(&args[2]);
            let disk = pilot::create_blank(pilot::pilot_geometry(pages), gen, &args[3])
                .expect("create_blank");
            let bytes = pilot::write_pdi(&disk, gen);
            let back = pdi::read(&bytes).expect("pdi read");
            let vol = pilot::read_volume(&back, gen).expect("read_volume");
            print_volume(&vol);
            println!("OK: round-trip consistent ({} bytes).", bytes.len());
        }
        _ => {
            eprintln!("usage:");
            eprintln!("  pilot_probe new <pages> <cedar|pilot> <name> <out.pdi>");
            eprintln!("  pilot_probe probe <file.pdi>");
            eprintln!("  pilot_probe add <file.pdi> <hostfile>");
            eprintln!("  pilot_probe del <file.pdi> <fileID-decimal>");
            eprintln!("  pilot_probe install-boot <file.pdi> <germ|bootfile|microcode> <hostfile>");
            eprintln!("  pilot_probe extract-boot <file.pdi> <germ|bootfile|microcode> <out>");
            eprintln!("  pilot_probe boot-info <file.pdi>");
            eprintln!("  pilot_probe roundtrip <pages> <cedar|pilot> <name>");
            std::process::exit(2);
        }
    }
}
