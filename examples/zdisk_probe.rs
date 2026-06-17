//! Validate our Pilot/Cedar reader against real Dwarf Draco `.zdisk` images.
//!
//! Usage: `zdisk_probe <file.zdisk> [<file.zdisk> ...]`
//!
//! Reads each `.zdisk` (applying a sibling `.zdelta` if present), dumps the
//! physical-volume root, runs a histogram of label attribute words across the
//! whole pack, and tries `pilot::read_volume`. This is the empirical check of
//! our (source-defined, previously un-oracled) Pilot format against ground
//! truth, so it prints raw diagnostics rather than assuming our model is right.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rusty_backup::fs::alto::pilot::{self, Generation, Label};
use rusty_backup::fs::alto::{zdisk, Disk};

fn rdw(buf: &[u8], wi: usize) -> u16 {
    ((buf[wi * 2] as u16) << 8) | (buf[wi * 2 + 1] as u16)
}

fn load(path: &Path) -> Result<Disk, String> {
    let bytes = fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut disk = zdisk::read(&bytes).map_err(|e| format!("zdisk parse: {e:?}"))?;
    let delta = path.with_extension(format!(
        "{}.zdelta",
        path.extension().and_then(|s| s.to_str()).unwrap_or("zdisk")
    ));
    // Dwarf names the delta "<full path>.zdelta", i.e. base + ".zdelta".
    let delta = path
        .to_str()
        .map(|s| format!("{s}.zdelta"))
        .map(std::path::PathBuf::from)
        .filter(|p| p.exists())
        .or(if delta.exists() { Some(delta) } else { None });
    if let Some(dp) = delta {
        let db = fs::read(&dp).map_err(|e| format!("read delta: {e}"))?;
        let n = zdisk::apply_delta(&mut disk, &db).map_err(|e| format!("delta: {e:?}"))?;
        println!("  applied delta {} ({n} sectors)", dp.display());
    }
    Ok(disk)
}

fn dump_page0(disk: &Disk) {
    let p = &disk.sector(0).unwrap().data;
    print!("  page0 words[0..12]:");
    for i in 0..12 {
        print!(" {:04x}", rdw(p, i));
    }
    println!();
    let seal = rdw(p, 0);
    let stored_cs = rdw(p, 255);
    let computed: Vec<u16> = (0..255).map(|i| rdw(p, i)).collect();
    let cs = pilot::pilot_checksum(&computed);
    println!(
        "  page0 seal={seal:#06x} (want 0o121212={:#06x})  version(w1)={}  word2={}  subvolCount(w64)={}",
        0o121212u16,
        rdw(p, 1),
        rdw(p, 2),
        rdw(p, 64)
    );
    println!(
        "  page0 checksum stored(w255)={stored_cs:#06x}  ourComputed={cs:#06x}  match={}",
        stored_cs == cs
    );
    print!("  page0 label words:");
    for i in 0..10 {
        print!(" {:04x}", rdw(&disk.sector(0).unwrap().label, i));
    }
    println!();
}

fn attr_histogram(disk: &Disk) {
    let mut hist: BTreeMap<u16, usize> = BTreeMap::new();
    let mut hist_swapped: BTreeMap<u16, usize> = BTreeMap::new();
    for s in &disk.sectors {
        let a = Label::parse(&s.label).attributes;
        *hist.entry(a).or_default() += 1;
        *hist_swapped.entry(a.swap_bytes()).or_default() += 1;
    }
    let top = |h: &BTreeMap<u16, usize>| -> Vec<(u16, usize)> {
        let mut v: Vec<_> = h.iter().map(|(k, c)| (*k, *c)).collect();
        v.sort_by_key(|x| std::cmp::Reverse(x.1));
        v.truncate(8);
        v
    };
    println!("  attribute word (w7) histogram, top 8 [as-stored]:");
    for (a, c) in top(&hist) {
        println!("    {a:#06x} ({a:5}) : {c}");
    }
    println!("  attribute word (w7) histogram, top 8 [byte-swapped]:");
    for (a, c) in top(&hist_swapped) {
        println!("    {a:#06x} ({a:5}) : {c}");
    }
    println!(
        "  (Cedar attr values: physicalRoot=1 badPageList=2 subVolMarker=4 logicalRoot=5 free=9728 header=9729 data=9730)"
    );
}

fn find_seals(disk: &Disk) {
    let seals = [
        ("physRoot 121212", 0o121212u16),
        ("logRoot 131313", 0o131313u16),
        ("subvolMarker 141414", 0o141414u16),
    ];
    for (name, seal) in seals {
        let mut hits = Vec::new();
        for (vda, s) in disk.sectors.iter().enumerate() {
            if rdw(&s.data, 0) == seal {
                hits.push(vda);
                if hits.len() >= 6 {
                    break;
                }
            }
        }
        println!("  data word0 == {name} ({seal:#06x}): first VDAs {hits:?}");
    }
}

fn try_read_volume(disk: &Disk) {
    for gen in [Generation::CedarNucleus, Generation::OriginalPilot] {
        match pilot::read_volume(disk, gen) {
            Ok(v) => {
                println!(
                    "  read_volume({gen:?}) OK: pv={:?} lv={:?} type={} size={} free={} vamFree={:?}",
                    v.pv_label, v.lv_label, v.volume_type, v.volume_size, v.free_pages, v.vam_free_pages
                );
                println!("    subvolumes:");
                for sv in &v.physical_root.sub_volumes {
                    println!(
                        "      lv_id={:?} lv_size={} lv_page={} pv_page={} n_pages={}",
                        sv.lv_id, sv.lv_size, sv.lv_page, sv.pv_page, sv.n_pages
                    );
                }
            }
            Err(e) => println!("  read_volume({gen:?}) ERR: {e:?}"),
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: zdisk_probe <file.zdisk> [...]");
        std::process::exit(2);
    }
    for arg in &args {
        let path = Path::new(arg);
        println!("\n=== {} ===", path.display());
        let disk = match load(path) {
            Ok(d) => d,
            Err(e) => {
                println!("  ERROR: {e}");
                continue;
            }
        };
        let g = &disk.geometry;
        println!(
            "  geometry: {} cyl x {} heads x {} sec = {} sectors ({} pages, {:.1} MB data)",
            g.n_cylinders,
            g.n_heads,
            g.n_sectors,
            g.total_sectors(),
            disk.sectors.len(),
            (disk.sectors.len() * 512) as f64 / 1_000_000.0
        );
        dump_page0(&disk);
        attr_histogram(&disk);
        find_seals(&disk);
        try_read_volume(&disk);
    }
}
