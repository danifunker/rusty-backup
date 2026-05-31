// Splice-and-verify recovery for a registry hive with a hole.
//
// A heavily fragmented hive can have interior $DATA clusters the lazy scan
// dropped (read back as zeros). Their bytes survive as "sentinel" runs in the
// GHO. This tool:
//   1. takes a hive already extracted with rb-cli get (it has the zero hole),
//   2. finds the zero gap inside the hive's used region,
//   3. tries each sentinel run whose size matches the gap, splicing its bytes
//      in and checking that the registry hbin chain stays consistent across
//      the patched region (hbin self-offsets are self-describing, so a wrong
//      splice breaks them — a real correctness check without ground truth),
//   4. writes the repaired hive when a splice validates.
//
// Usage:
//   cargo run --release --example gho_hive_splice -- PATH.GHO CORRUPT_HIVE OUT_HIVE

use rusty_backup::rbformats::gho::GhoReader;

fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

// Validate the hbin chain over [4096, used_end). Returns the count of valid
// hbins, and whether every hbin in range was consistent (magic + self-offset +
// size multiple of 4096). A correct hive walks cleanly; a bad splice trips it.
fn validate_hbins(hive: &[u8], used_end: usize) -> (usize, bool) {
    let mut pos = 4096usize;
    let mut count = 0;
    while pos + 32 <= used_end {
        if &hive[pos..pos + 4] != b"hbin" {
            return (count, false);
        }
        let self_off = rd_u32(hive, pos + 4) as usize; // offset from first hbin
        let size = rd_u32(hive, pos + 8) as usize;
        if self_off != pos - 4096 || size < 4096 || !size.is_multiple_of(4096) {
            return (count, false);
        }
        count += 1;
        pos += size;
    }
    (count, true)
}

// Strong check: within each hbin that overlaps [range_start, range_end), walk
// the registry cell chain. Cells are size-prefixed (i32 at the cell start; the
// magnitude is the cell length, always a non-zero multiple of 8) and must tile
// the hbin's body (from +0x20 to +size) EXACTLY. Correct spliced data keeps the
// cells tiling; wrong data yields a bad size or a chain that overshoots/under-
// shoots the hbin boundary. This is the real test that the patched bytes are
// the hive's actual data, not just non-zero filler.
fn validate_cells_overlapping(
    hive: &[u8],
    range_start: usize,
    range_end: usize,
    used_end: usize,
) -> bool {
    let mut pos = 4096usize;
    while pos + 32 <= used_end {
        if &hive[pos..pos + 4] != b"hbin" {
            return false;
        }
        let size = rd_u32(hive, pos + 8) as usize;
        if size < 4096 || !size.is_multiple_of(4096) {
            return false;
        }
        let hbin_end = pos + size;
        // Only the hbins overlapping the patched range need their cells checked.
        if pos < range_end && hbin_end > range_start {
            let mut c = pos + 0x20;
            while c < hbin_end {
                if c + 4 > hive.len() {
                    return false;
                }
                let raw = i32::from_le_bytes([hive[c], hive[c + 1], hive[c + 2], hive[c + 3]]);
                let cell = raw.unsigned_abs() as usize;
                if cell == 0 || !cell.is_multiple_of(8) || c + cell > hbin_end {
                    return false;
                }
                c += cell;
            }
            if c != hbin_end {
                return false; // cells must tile the hbin exactly
            }
        }
        pos = hbin_end;
    }
    true
}

// First all-zero 4 KiB cluster run at or after `from`, as (offset, length).
fn find_zero_gap(hive: &[u8], from: usize, used_end: usize) -> Option<(usize, usize)> {
    let mut o = from.max(4096);
    while o + 4096 <= used_end {
        if hive[o..o + 512].iter().all(|&x| x == 0) {
            let start = o;
            let mut end = o;
            while end + 4096 <= used_end && hive[end..end + 512].iter().all(|&x| x == 0) {
                end += 4096;
            }
            return Some((start, end - start));
        }
        o += 4096;
    }
    None
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let gho = args.next().expect("GHO path");
    let corrupt = args.next().expect("corrupt hive path");
    let out = args.next().expect("output hive path");

    let mut hive = std::fs::read(&corrupt)?;
    if &hive[0..4] != b"regf" {
        anyhow::bail!("not a hive (no regf): {corrupt}");
    }
    let used = rd_u32(&hive, 0x28) as usize + 4096;
    println!("corrupt hive: size={} used_end={used}", hive.len());

    let mut r = GhoReader::open(std::path::Path::new(&gho))?;
    r.prepare_full_image();
    let mut sentinels = r.debug_sentinel_runs_full(1 << 20); // up to 1 MiB per run
    println!("sentinel runs available: {}", sentinels.len());

    // Fix every zero gap. Use a cursor so unrecoverable gaps (left as clean
    // zeros) don't re-trigger the scan. We always write a BEST-EFFORT hive at
    // the end so it can be `reg load`-tested — even one with residual holes.
    let mut gaps_fixed = 0;
    let mut gaps_unrecovered = 0;
    let mut cursor = 4096usize;
    while let Some((gap_off, gap_len)) = find_zero_gap(&hive, cursor, used) {
        println!(
            "gap at offset {gap_off} length {gap_len} ({} clusters)",
            gap_len / 4096
        );
        let mut chosen: Option<(usize, usize)> = None; // (sentinel idx, window offset)
        'outer: for (i, (_cc, _seq, data)) in sentinels.iter().enumerate() {
            if data.is_empty() {
                continue;
            }
            let mut w = 0usize;
            while w + gap_len <= data.len() {
                let mut trial = hive.clone();
                trial[gap_off..gap_off + gap_len].copy_from_slice(&data[w..w + gap_len]);
                if validate_cells_overlapping(&trial, gap_off, gap_off + gap_len, used) {
                    hive = trial;
                    chosen = Some((i, w));
                    break 'outer;
                }
                w += 4096;
            }
        }
        match chosen {
            Some((i, w)) => {
                println!("  -> filled from sentinel #{i} window@{w} (cell chain tiles exactly)");
                sentinels[i].2[w..w + gap_len].fill(0);
                gaps_fixed += 1;
                // re-scan from the start in case fill exposed nothing new
                cursor = 4096;
            }
            None => {
                println!("  -> NO sentinel fits this gap; leaving it zero-filled");
                gaps_unrecovered += 1;
                cursor = gap_off + gap_len; // skip past it
            }
        }
    }

    println!("\ngaps fixed: {gaps_fixed}  gaps unrecovered: {gaps_unrecovered}");
    let whole_ok = validate_cells_overlapping(&hive, 4096, used, used);
    let (hcount, hok) = validate_hbins(&hive, used);
    println!("final: whole-hive cells_tile={whole_ok} hbin_chain_ok={hok} hbins={hcount}");
    std::fs::write(&out, &hive)?;
    if gaps_unrecovered == 0 && whole_ok && hok {
        println!("RECOVERED (complete): {out}");
    } else {
        println!(
            "WROTE BEST-EFFORT hive ({gaps_unrecovered} hole(s) still zero) -> {out}\n\
             Try `reg load` on it; if Windows accepts it the residual holes are in\n\
             non-critical/unreferenced cells."
        );
    }
    Ok(())
}
