// Find runs in the GhoReader index that overlap each other's LCN range.
// Overlapping runs (different lcn_start, but covering the same LCN somewhere)
// confuse `binary_search_by` — it returns any one match, so reads at the
// overlap point are nondeterministic.
//
// Usage:
//   cargo run --release --example gho_ntfs_index_overlap -- PATH.GHO
//
// Reads ntfs_mapped_ranges() (only filled after prepare_full_image()) and
// reports any adjacent ranges where end_a > start_b.

use rusty_backup::rbformats::gho::GhoReader;

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).expect("path to GHO");
    let mut r = GhoReader::open(std::path::Path::new(&path))?;
    r.prepare_full_image();

    let Some(ranges) = r.ntfs_mapped_ranges() else {
        println!("not an NTFS file-aware GHO");
        return Ok(());
    };

    println!("total runs: {}", ranges.len());
    let mut sorted = ranges.clone();
    sorted.sort_by_key(|&(lcn, _)| lcn);

    let mut overlaps = 0;
    for w in sorted.windows(2) {
        let (la, ca) = w[0];
        let (lb, _cb) = w[1];
        let end_a = la + ca;
        if end_a > lb {
            overlaps += 1;
            if overlaps <= 20 {
                println!("OVERLAP: run lcn={la} cc={ca} (ends at {end_a}) vs next lcn={lb}");
            }
        }
    }
    println!("total overlaps: {overlaps}");

    // Also check: any clusters in the "doubly mapped" zone?
    let mut by_lcn: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for &(lcn, cc) in &sorted {
        for c in 0..cc {
            *by_lcn.entry(lcn + c).or_insert(0) += 1;
        }
    }
    let mut multi: Vec<_> = by_lcn.iter().filter(|(_, &v)| v > 1).collect();
    multi.sort();
    println!("clusters mapped by > 1 run: {}", multi.len());
    for (lcn, n) in multi.iter().take(20) {
        println!("  lcn={lcn} mapped by {n} runs");
    }
    Ok(())
}
