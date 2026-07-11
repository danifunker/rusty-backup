//! Build a "kitchen sink" Cedar 6.1 Dorado boot disk: the verified BasicCedar
//! germ + boot payloads, then as much of the `cedarchest6.1` package corpus as
//! fits a ~31 MB Pilot volume, with the headliners named in the Cedar `client`
//! name directory (games first).
//!
//! This drives the same `pilot.rs` API as `pilot_probe` (create_blank /
//! install_boot_file / add_file / set_client_directory / write_pdi) but in a
//! single process, so 2000+ files add against an in-memory `Disk` instead of
//! re-reading the whole PDI per file (which the `pilot_probe add` CLI does).
//!
//! Usage:
//!   cedar_kitchen_sink <germ> <boot> <manifest.tsv> <out.pdi>
//!
//! Each manifest line is tab-separated: `tag  size  topdir  name  hostpath`,
//! where `tag` is `game` or `rest` and `topdir` is the package directory. The
//! manifest is expected pre-ordered (games first, then ascending size) so the
//! greedy fill guarantees the games land and maximizes package count.

use std::collections::HashSet;
use std::fs;

use rusty_backup::fs::alto::pilot::{self, Generation, PvBootFile};

/// Volume size in Pilot pages. `pilot_geometry` takes a `u16`, so 65535 pages
/// (~31 MB usable after boot + leaders + VAM) is the ceiling for one volume.
const VOLUME_PAGES: u16 = 65535;

/// The main launcher `.bcd` for each game directory — named explicitly so the
/// game shows by its real name rather than an arbitrary first-file-in-dir.
const GAME_LAUNCHERS: &[&str] = &[
    "MazeWar.BCD",
    "ChessHackImpl.bcd",
    "Football.bcd",
    "PigsInSpace.bcd",
    "Celtics.bcd",
];

/// Non-`.bcd` files to name by exact name when present (boot/runtime support).
const EXACT_NAMED: &[&str] = &["User.Profile", "User.profile"];

/// Marquee application directories to name first (after the games), so the
/// browse view shows recognizable Cedar apps rather than arbitrary modules.
/// For each, the `.bcd` whose stem matches the directory name is preferred.
const HEADLINER_DIRS: &[&str] = &[
    "draw2d",
    "chat",
    "newclock",
    "clock",
    "gargoyle",
    "whiteboard",
    "tajo",
    "math",
    "mathlib",
    "sil",
    "ftp",
    "grep",
    "ls",
    "iconeditor",
    "fontedit",
    "print",
    "interpress",
    "peanut",
    "cypress",
    "hostname",
    "finger",
    "newcalc",
    "texture2d",
    "threedworld",
    "geometry3d",
    "colortool",
    "fig",
    "tex",
    "scanner",
    "magnifier",
    "spellingtool",
    "fontedit",
];

/// Strip a trailing IFS `!version` suffix: `MazeWar.BCD!10` -> `MazeWar.BCD`.
fn strip_ver(name: &str) -> String {
    name.split('!').next().unwrap_or(name).to_string()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        eprintln!("usage: cedar_kitchen_sink <germ> <boot> <manifest.tsv> <out.pdi>");
        std::process::exit(2);
    }
    let (germ_p, boot_p, manifest_p, out_p) = (&args[0], &args[1], &args[2], &args[3]);
    let gen = Generation::CedarNucleus;

    let germ = fs::read(germ_p).expect("read germ");
    let boot = fs::read(boot_p).expect("read boot");

    // Blank Cedar volume, then install the two boot payloads into the reserved
    // boot chain (these consume volume pages, so add the files afterwards).
    let mut disk =
        pilot::create_blank(pilot::pilot_geometry(VOLUME_PAGES), gen, "CedarKitchenSink")
            .expect("create_blank");
    disk = pilot::install_boot_file(&disk, gen, PvBootFile::Germ, &germ).expect("install germ");
    disk = pilot::install_boot_file(&disk, gen, PvBootFile::BootFile, &boot).expect("install boot");
    println!(
        "Installed germ ({} B, {} pages) + BasicCedar boot ({} B, {} pages).",
        germ.len(),
        germ.len().div_ceil(512),
        boot.len(),
        boot.len().div_ceil(512),
    );

    // Add every manifest file until the volume reports full.
    let manifest = fs::read_to_string(manifest_p).expect("read manifest");
    let mut added: Vec<(String, String, u32)> = Vec::new(); // (topdir, name, fileID)
    let mut added_bytes: u64 = 0;
    let mut stopped = false;
    for (i, line) in manifest.lines().enumerate() {
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 5 {
            continue;
        }
        let (top, name, path) = (cols[2], cols[3], cols[4]); // tag,size,[top],[name],[path]
        let payload = match fs::read(path) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("skip {path}: {e}");
                continue;
            }
        };
        match pilot::add_file(&disk, gen, &payload) {
            Ok((d, fid)) => {
                disk = d;
                added_bytes += payload.len() as u64;
                added.push((top.to_string(), name.to_string(), fid));
            }
            Err(e) => {
                eprintln!(
                    "volume full after {} files (next was {name}): {e}",
                    added.len()
                );
                stopped = true;
                break;
            }
        }
        if (i + 1) % 250 == 0 {
            println!("  ... {} files added", added.len());
        }
    }
    println!(
        "Added {} files ({:.1} MB){}.",
        added.len(),
        added_bytes as f64 / 1_048_576.0,
        if stopped { " [hit volume limit]" } else { "" }
    );

    // Pick headliners to name: game launchers first, then one representative
    // `.bcd` per package directory, for broad coverage. The single-leaf writer
    // caps at ~50 names, so over-provision candidates and let the writer's
    // overflow error drive a trim-and-retry to the true leaf capacity.
    let mut candidates: Vec<(String, u32)> = Vec::new();
    let mut named_tops: HashSet<String> = HashSet::new();
    let mut named_names: HashSet<String> = HashSet::new();
    // Exact-name headliners: the game launchers, plus boot-support files worth
    // surfacing by name (e.g. User.Profile). Named first so they survive the
    // leaf-capacity trim below.
    for wanted in GAME_LAUNCHERS.iter().chain(EXACT_NAMED.iter()) {
        if let Some((top, name, fid)) = added
            .iter()
            .find(|(_, name, _)| strip_ver(name).eq_ignore_ascii_case(wanted))
        {
            let sname = strip_ver(name);
            if named_names.insert(sname.to_ascii_uppercase()) {
                candidates.push((sname, *fid));
                named_tops.insert(top.clone());
            }
        }
    }
    // For a package directory, pick the `.bcd` whose stem matches the dir name
    // (the main package, e.g. `draw2d` -> `Draw2d.bcd`); else the first `.bcd`.
    let pick_for_dir = |top: &str| -> Option<(String, u32)> {
        let in_dir: Vec<&(String, String, u32)> =
            added.iter().filter(|(t, _, _)| t == top).collect();
        let matches_dir = |name: &str| {
            strip_ver(name)
                .to_ascii_lowercase()
                .strip_suffix(".bcd")
                .map(|stem| stem.eq_ignore_ascii_case(top))
                .unwrap_or(false)
        };
        let is_bcd = |name: &str| strip_ver(name).to_ascii_lowercase().ends_with(".bcd");
        // Prefer the runnable object named after the dir, then any .bcd; for
        // source-only dirs (e.g. a disk of pure .mesa) fall back to the
        // dir-named source file, then the first file, so it still browses.
        let chosen = in_dir
            .iter()
            .find(|(_, name, _)| is_bcd(name) && matches_dir(name))
            .or_else(|| in_dir.iter().find(|(_, name, _)| is_bcd(name)))
            .or_else(|| in_dir.iter().find(|(_, name, _)| matches_dir(name)))
            .or_else(|| in_dir.first())?;
        Some((strip_ver(&chosen.1), chosen.2))
    };
    let try_name = |top: &str,
                    candidates: &mut Vec<(String, u32)>,
                    named_tops: &mut HashSet<String>,
                    named_names: &mut HashSet<String>| {
        if candidates.len() >= 60 || named_tops.contains(top) {
            return;
        }
        if let Some((sname, fid)) = pick_for_dir(top) {
            if named_names.contains(&sname.to_ascii_uppercase()) {
                return;
            }
            named_tops.insert(top.to_string());
            named_names.insert(sname.to_ascii_uppercase());
            candidates.push((sname, fid));
        }
    };
    // Marquee apps first, then fill from the remaining dirs in add order.
    for top in HEADLINER_DIRS {
        try_name(top, &mut candidates, &mut named_tops, &mut named_names);
    }
    let mut seen_dirs: HashSet<String> = HashSet::new();
    for (top, _, _) in &added {
        if seen_dirs.insert(top.clone()) {
            try_name(top, &mut candidates, &mut named_tops, &mut named_names);
        }
    }

    // Trim-and-retry until the name directory fits one B-tree leaf.
    let mut names = candidates.clone();
    let disk_named = loop {
        let entries: Vec<(String, u16, u32)> =
            names.iter().map(|(n, f)| (n.clone(), 1u16, *f)).collect();
        match pilot::set_client_directory(&disk, gen, &entries) {
            Ok(d) => break d,
            Err(e) => {
                if names.len() <= 1 {
                    eprintln!("could not write even one name entry: {e}");
                    break disk.clone();
                }
                names.pop();
            }
        }
    };
    println!(
        "Named {} headliners in the Cedar client directory (of {} candidates):",
        names.len(),
        candidates.len()
    );
    for (n, _) in &names {
        println!("    {n}");
    }

    let bytes = pilot::write_pdi(&disk_named, gen);
    fs::write(out_p, &bytes).expect("write pdi");
    println!("Wrote {out_p} ({} bytes).", bytes.len());
}
