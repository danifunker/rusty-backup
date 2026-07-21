//! Repack a Cedar PDI authored by the historical one-header-page writer into
//! a Cedar-valid volume while preserving as many user files and names as fit.
//!
//! Usage:
//!   cedar_repack <old.pdi> <germ> <boot> <new.pdi> [max-files]
//!                [--crc-names <file>] [--names <dir>]
//!
//! `--names <dir>` recovers filenames by CONTENT: many Cedar corpus volumes
//! carry a file's bytes but not its name (the historical writer named only a
//! handful of headliners). If a copied file's content -- padded up to a page
//! boundary -- matches a file under <dir>, that file's base name is used. The
//! payload came from an archive of real files, so this is exact, not fuzzy.
//!
//! `--crc-names <file>` recovers filenames by SIZE + CRC-32 from a TSV index,
//! one `crc32hex<TAB>size<TAB>name` line per entry. The CHM PARC cross-
//! reference already lists every file's exact byte size and its IEEE CRC-32,
//! so names come straight from that listing with no downloads -- it recovers
//! far more of the corpus than the local content match can. If both flags are
//! given, the CRC index is tried first and `--names` is the fallback.

use std::collections::HashMap;
use std::fs;

use rusty_backup::fs::alto::pdi;
use rusty_backup::fs::alto::pilot::{self, Generation, PilotFilesystem, PvBootFile};
use rusty_backup::fs::filesystem::Filesystem;

/// Index of <dir>'s files by exact byte length -> [(basename, content)]. A
/// corpus file is the archive file padded with zeros to a 512-byte page, so a
/// match is `corpus[..L] == archive` with an all-zero tail and `L` within one
/// page of the corpus length.
fn build_name_index(dir: &str) -> HashMap<usize, Vec<(String, Vec<u8>)>> {
    let mut idx: HashMap<usize, Vec<(String, Vec<u8>)>> = HashMap::new();
    let mut stack = vec![std::path::PathBuf::from(dir)];
    while let Some(p) = stack.pop() {
        let rd = match fs::read_dir(&p) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(b) = fs::read(&path) {
                if b.is_empty() {
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy();
                let base = name.split('!').next().unwrap_or(&name).to_string();
                idx.entry(b.len()).or_default().push((base, b));
            }
        }
    }
    idx
}

/// Recover `data`'s name from the content index, or None.
fn recover_name(data: &[u8], idx: &HashMap<usize, Vec<(String, Vec<u8>)>>) -> Option<String> {
    let p = data.len();
    for l in (p.saturating_sub(511)..=p).rev() {
        if let Some(cands) = idx.get(&l) {
            if data[l..].iter().any(|&b| b != 0) {
                continue;
            }
            for (base, content) in cands {
                if &data[..l] == content.as_slice() {
                    return Some(base.clone());
                }
            }
        }
    }
    None
}

/// Index a CHM cross-reference TSV -> name keyed on (byte length, CRC-32). Each
/// line is `crc32hex<TAB>size<TAB>name`: crc32hex is 8 lowercase hex digits,
/// size is the decimal byte length. First occurrence of a (size, crc) pair
/// wins; malformed lines are ignored. The archive listing already carries every
/// file's exact size and IEEE CRC-32, so names come straight from it.
fn build_crc_index(path: &str) -> HashMap<(u32, u32), String> {
    let mut idx: HashMap<(u32, u32), String> = HashMap::new();
    let text = fs::read_to_string(path).expect("read --crc-names file");
    for line in text.lines() {
        let mut cols = line.split('\t');
        let (Some(crc_hex), Some(size_str), Some(name)) = (cols.next(), cols.next(), cols.next())
        else {
            continue;
        };
        let (Ok(crc), Ok(size)) = (u32::from_str_radix(crc_hex, 16), size_str.parse::<u32>())
        else {
            continue;
        };
        idx.entry((size, crc)).or_insert_with(|| name.to_string());
    }
    idx
}

/// Recover `data`'s name from the (byte length, CRC-32) index, or None. Like
/// `recover_name`, the on-disk `data` is the real file padded with zeros to a
/// 512-byte page, so try each length whose tail is all zero and match its
/// CRC-32 against the index; the first hit gives the name.
fn recover_crc_name(data: &[u8], idx: &HashMap<(u32, u32), String>) -> Option<String> {
    let p = data.len();
    for l in (p.saturating_sub(511)..=p).rev() {
        if data[l..].iter().any(|&b| b != 0) {
            continue;
        }
        let crc = crc32fast::hash(&data[..l]);
        if let Some(name) = idx.get(&(l as u32, crc)) {
            return Some(name.clone());
        }
    }
    None
}

const VOLUME_PAGES: u16 = 65535;

fn is_old_boot_payload(name: &str) -> bool {
    name.starts_with("LV0_00020000_") || name.starts_with("LV0_00030000_")
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let names_dir = args.iter().position(|a| a == "--names").map(|i| {
        let d = args.get(i + 1).expect("--names needs a directory").clone();
        args.drain(i..=i + 1);
        d
    });
    let name_index = names_dir.as_deref().map(build_name_index);
    if let Some(idx) = &name_index {
        println!(
            "name index: {} files under {}",
            idx.values().map(|v| v.len()).sum::<usize>(),
            names_dir.as_deref().unwrap()
        );
    }
    let crc_names_file = args.iter().position(|a| a == "--crc-names").map(|i| {
        let f = args.get(i + 1).expect("--crc-names needs a file").clone();
        args.drain(i..=i + 1);
        f
    });
    let crc_index = crc_names_file.as_deref().map(build_crc_index);
    if let Some(idx) = &crc_index {
        println!(
            "crc index: {} entries from {}",
            idx.len(),
            crc_names_file.as_deref().unwrap()
        );
    }
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
                } else if let Some(name) = crc_index
                    .as_ref()
                    .and_then(|idx| recover_crc_name(&data, idx))
                    .or_else(|| name_index.as_ref().and_then(|idx| recover_name(&data, idx)))
                {
                    names.push((name, 1, fid));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32fast_is_zlib_ieee_crc32() {
        // Canonical CRC-32/ISO-HDLC (zlib/IEEE) check value for "123456789".
        // Confirms crc32fast::hash is the same CRC-32 the CHM listing records.
        assert_eq!(crc32fast::hash(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn recover_crc_name_matches_zero_padded_page() {
        let payload = b"hello cedar crc index";
        let size = payload.len() as u32;
        let crc = crc32fast::hash(payload);
        let mut idx: HashMap<(u32, u32), String> = HashMap::new();
        idx.insert((size, crc), "Greeting.mesa".to_string());

        // On disk the file is the payload padded with zeros to a 512-byte page.
        let mut data = payload.to_vec();
        data.resize(512, 0);
        assert_eq!(
            recover_crc_name(&data, &idx).as_deref(),
            Some("Greeting.mesa")
        );

        // A file whose (size, crc) is not in the index recovers nothing.
        let mut other = b"different bytes entirely".to_vec();
        other.resize(512, 0);
        assert_eq!(recover_crc_name(&other, &idx), None);
    }
}
