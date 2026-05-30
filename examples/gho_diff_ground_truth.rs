// Compare a GHO-reconstructed raw image against the ground-truth raw image
// decoded from the original source (e.g. a vmdk). Both must be whole-disk
// images with the NTFS partition at the same LBA.
//
// Reports:
//   - per-system-MFT-record byte equality (records 0..32)
//   - $MFTMirr ($DATA) location + whether our content matches ground truth
//   - a grouped map of mismatching clusters across the whole volume, each
//     classified by content magic (INDX / FILE / SD / zeros / other)
//
// Usage:
//   cargo run --release --example gho_diff_ground_truth -- GROUND.img OURS.img [PART_LBA]

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn rd_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

fn read_at(f: &mut File, off: u64, len: usize) -> Vec<u8> {
    let mut b = vec![0u8; len];
    f.seek(SeekFrom::Start(off)).unwrap();
    let mut got = 0;
    while got < len {
        match f.read(&mut b[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => break,
        }
    }
    b.truncate(got);
    b
}

fn classify(b: &[u8]) -> &'static str {
    if b.len() < 4 {
        return "short";
    }
    if b.iter().all(|&x| x == 0) {
        return "zeros";
    }
    match &b[0..4] {
        m if m == b"INDX" => "INDX",
        m if m == b"FILE" => "FILE",
        m if m == b"RSTR" => "RSTR",
        m if m == b"RCRD" => "RCRD",
        m if m == b"BAAD" => "BAAD",
        _ => {
            if b[0] == 0x01 && b[1] == 0x00 && (b[3] & 0x80) != 0 {
                "SD"
            } else {
                "data"
            }
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let gt_path = args.next().expect("ground-truth image");
    let our_path = args.next().expect("our image");
    let part_lba: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(63);

    let mut gt = File::open(&gt_path).unwrap();
    let mut our = File::open(&our_path).unwrap();

    let part_off = part_lba * 512;

    // Read VBRs.
    let gt_vbr = read_at(&mut gt, part_off, 512);
    let our_vbr = read_at(&mut our, part_off, 512);

    let bps = rd_u16(&gt_vbr, 0x0B) as u64;
    let spc = gt_vbr[0x0D] as u64;
    let cluster = bps * spc;
    let total_sectors = rd_u64(&gt_vbr, 0x28);
    let mft_lcn = rd_u64(&gt_vbr, 0x30);
    let mftmirr_lcn = rd_u64(&gt_vbr, 0x38);
    let cmft = gt_vbr[0x40] as i8;
    let rec_size = if cmft < 0 {
        1u64 << (-cmft as u32)
    } else {
        cmft as u64 * cluster
    };

    println!("=== VBR (ground truth) ===");
    println!("bps={bps} spc={spc} cluster={cluster} total_sectors={total_sectors}");
    println!("mft_lcn={mft_lcn} mftmirr_lcn={mftmirr_lcn} rec_size={rec_size}");
    println!();
    println!("VBR bytes equal (GT vs ours): {}", gt_vbr == our_vbr);
    if gt_vbr != our_vbr {
        // Find which bytes differ.
        let mut diffs = Vec::new();
        for i in 0..gt_vbr.len().min(our_vbr.len()) {
            if gt_vbr[i] != our_vbr[i] {
                diffs.push(i);
            }
        }
        println!("  VBR differing offsets: {diffs:?}");
        for &o in diffs.iter().take(20) {
            println!(
                "    @0x{o:02x}: GT={:02x} OURS={:02x}",
                gt_vbr[o], our_vbr[o]
            );
        }
    }
    println!();

    // --- Compare system MFT records 0..32 ---
    println!("=== System MFT records (0..32) byte-equality ===");
    let mft_byte = part_off + mft_lcn * cluster;
    for rec in 0..32u64 {
        let off = mft_byte + rec * rec_size;
        let gt_rec = read_at(&mut gt, off, rec_size as usize);
        let our_rec = read_at(&mut our, off, rec_size as usize);
        if &gt_rec[0..4] != b"FILE" && &our_rec[0..4] != b"FILE" {
            continue;
        }
        let eq = gt_rec == our_rec;
        let name = match rec {
            0 => "$MFT",
            1 => "$MFTMirr",
            2 => "$LogFile",
            3 => "$Volume",
            4 => "$AttrDef",
            5 => ". (root)",
            6 => "$Bitmap",
            7 => "$Boot",
            8 => "$BadClus",
            9 => "$Secure",
            10 => "$UpCase",
            11 => "$Extend",
            _ => "",
        };
        let marker = if eq { "OK  " } else { "DIFF" };
        let gt_magic = classify(&gt_rec);
        let our_magic = classify(&our_rec);
        println!(
            "  rec#{rec:<3} {marker} {name:<12} GT={gt_magic} OURS={our_magic}{}",
            if eq {
                String::new()
            } else {
                // count differing bytes
                let n = gt_rec
                    .iter()
                    .zip(our_rec.iter())
                    .filter(|(a, b)| a != b)
                    .count();
                format!("  ({n} bytes differ)")
            }
        );
    }
    println!();

    // --- $MFTMirr cluster comparison ---
    println!("=== $MFTMirr cluster (lcn={mftmirr_lcn}) ===");
    let mm_off = part_off + mftmirr_lcn * cluster;
    let gt_mm = read_at(&mut gt, mm_off, cluster as usize);
    let our_mm = read_at(&mut our, mm_off, cluster as usize);
    println!(
        "  GT magic={} OURS magic={}",
        classify(&gt_mm),
        classify(&our_mm)
    );
    println!("  content equal: {}", gt_mm == our_mm);
    println!(
        "  GT  first 32: {}",
        gt_mm[..32]
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!(
        "  OUR first 32: {}",
        our_mm[..32]
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!();

    // --- Whole-volume cluster mismatch map ---
    // Only compares allocated-looking clusters: skip clusters that are zeros in
    // BOTH (free space). Group consecutive mismatching LCNs into runs.
    println!("=== Cluster mismatch map (allocated clusters only) ===");
    let total_clusters = total_sectors * bps / cluster;
    let chunk_clusters = 256usize;

    let mut lcn = 0u64;
    let mut mismatches: u64 = 0;
    let mut compared: u64 = 0;
    let mut both_zero: u64 = 0;
    // Grouped runs: (start_lcn, len, gt_magic, our_magic)
    let mut runs: Vec<(u64, u64, &'static str, &'static str)> = Vec::new();

    while lcn < total_clusters {
        let want = chunk_clusters.min((total_clusters - lcn) as usize);
        let off = part_off + lcn * cluster;
        let gt_buf = read_at(&mut gt, off, want * cluster as usize);
        let our_buf = read_at(&mut our, off, want * cluster as usize);
        let avail = (gt_buf.len().min(our_buf.len())) / cluster as usize;

        for i in 0..avail {
            let cs = i * cluster as usize;
            let ce = cs + cluster as usize;
            let gtc = &gt_buf[cs..ce];
            let ourc = &our_buf[cs..ce];
            let this_lcn = lcn + i as u64;

            let gt_zero = gtc.iter().all(|&x| x == 0);
            let our_zero = ourc.iter().all(|&x| x == 0);
            if gt_zero && our_zero {
                both_zero += 1;
                continue;
            }
            compared += 1;
            if gtc == ourc {
                continue;
            }
            mismatches += 1;
            let gm = classify(gtc);
            let om = classify(ourc);
            // Extend the last run if contiguous and same classification.
            if let Some(last) = runs.last_mut() {
                if last.0 + last.1 == this_lcn && last.2 == gm && last.3 == om {
                    last.1 += 1;
                    continue;
                }
            }
            runs.push((this_lcn, 1, gm, om));
        }
        lcn += avail as u64;
        if avail < want {
            break; // EOF on one of the files
        }
    }

    println!("total_clusters={total_clusters}");
    println!("both-zero (free) clusters skipped: {both_zero}");
    println!("allocated clusters compared: {compared}");
    println!("mismatching clusters: {mismatches}");
    println!("mismatch runs: {}", runs.len());
    println!();
    println!("First 60 mismatch runs (lcn, len, GT->OURS):");
    for (start, len, gm, om) in runs.iter().take(60) {
        println!("  lcn {start:>10} len {len:>6}  {gm} -> {om}");
    }
    if runs.len() > 60 {
        println!("  ... and {} more runs", runs.len() - 60);
    }

    // Summary by (gt,our) classification pair.
    use std::collections::BTreeMap;
    let mut by_pair: BTreeMap<(&str, &str), (u64, u64)> = BTreeMap::new(); // pair -> (run_count, cluster_count)
    for (_, len, gm, om) in &runs {
        let e = by_pair.entry((gm, om)).or_insert((0, 0));
        e.0 += 1;
        e.1 += len;
    }
    println!();
    println!("Mismatch summary by classification (GT magic -> OURS magic):");
    for ((gm, om), (rc, cc)) in &by_pair {
        println!("  {gm:>6} -> {om:<6}: {rc:>6} runs, {cc:>8} clusters");
    }
}
