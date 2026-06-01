// Audit the reconstruction for the run-detection gap: clusters the MFT
// declares as non-resident attribute data but which no mapped run covers
// (so they read back as zeros). Groups the gap by owning MFT record and,
// for the worst offenders, prints the file name.
//
// Usage:
//   cargo run --release --example gho_ntfs_gap_audit -- PATH.GHO

use rusty_backup::rbformats::gho::GhoReader;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn rd_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn apply_fixup(rec: &mut [u8], bps: usize) {
    let usa_off = rd_u16(rec, 4) as usize;
    let usa_cnt = rd_u16(rec, 6) as usize;
    if usa_cnt == 0 || usa_off + usa_cnt * 2 > rec.len() {
        return;
    }
    let usn = [rec[usa_off], rec[usa_off + 1]];
    for i in 1..usa_cnt {
        let se = i * bps - 2;
        if se + 2 > rec.len() {
            break;
        }
        if rec[se] == usn[0] && rec[se + 1] == usn[1] {
            rec[se] = rec[usa_off + i * 2];
            rec[se + 1] = rec[usa_off + i * 2 + 1];
        }
    }
}
fn read_at(r: &mut GhoReader, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut b = vec![0u8; len];
    r.seek(SeekFrom::Start(off))?;
    r.read_exact(&mut b)?;
    Ok(b)
}
// Decode a runlist into absolute (lcn, length) pairs, skipping sparse holes.
fn decode_runs(data: &[u8]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut lcn: i64 = 0;
    while i < data.len() {
        let hdr = data[i];
        if hdr == 0 {
            break;
        }
        let len_sz = (hdr & 0x0F) as usize;
        let off_sz = (hdr >> 4) as usize;
        i += 1;
        if len_sz == 0 || i + len_sz + off_sz > data.len() {
            break;
        }
        let mut length: u64 = 0;
        for j in 0..len_sz {
            length |= (data[i + j] as u64) << (8 * j);
        }
        i += len_sz;
        if off_sz > 0 {
            let mut off: i64 = 0;
            for j in 0..off_sz {
                off |= (data[i + j] as i64) << (8 * j);
            }
            let shift = 64 - off_sz * 8;
            off = (off << shift) >> shift;
            i += off_sz;
            lcn += off;
            if lcn > 0 {
                out.push((lcn as u64, length));
            }
        }
        // offset_size==0 => sparse hole, no real LCN; skip.
    }
    out
}
fn file_name(rec: &[u8]) -> Option<String> {
    let mut ao = rd_u16(rec, 0x14) as usize;
    while ao + 16 <= rec.len() {
        let at = rd_u32(rec, ao);
        if at == 0xFFFF_FFFF || at == 0 {
            break;
        }
        let al = rd_u32(rec, ao + 4) as usize;
        if al < 16 || ao + al > rec.len() {
            break;
        }
        if at == 0x30 {
            let vo = rd_u16(rec, ao + 0x14) as usize;
            let base = ao + vo;
            if base + 0x42 <= rec.len() {
                let name_len = rec[base + 0x40] as usize;
                let mut s = String::new();
                for i in 0..name_len {
                    let c = rd_u16(rec, base + 0x42 + i * 2);
                    if let Some(ch) = char::from_u32(c as u32) {
                        s.push(ch);
                    }
                }
                return Some(s);
            }
        }
        ao += al;
    }
    None
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let path = std::env::args().nth(1).expect("path to GHO");
    let mut r = GhoReader::open(std::path::Path::new(&path))?;
    r.prepare_full_image();

    // Mapped LCN ranges -> a sorted Vec for binary-search coverage tests.
    let mapped = r.ntfs_mapped_ranges().expect("not an NTFS file-aware GHO");
    let mut mapped = mapped;
    mapped.sort_by_key(|r| r.0);
    let covered = |lcn: u64| -> bool {
        // binary search for a range [start, start+len) containing lcn
        mapped
            .binary_search_by(|&(s, l)| {
                if lcn < s {
                    std::cmp::Ordering::Greater
                } else if lcn >= s + l {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    };

    let vbr = read_at(&mut r, 0, 512)?;
    let bps = rd_u16(&vbr, 0x0B) as usize;
    let spc = vbr[0x0D] as usize;
    let cluster = (bps * spc) as u64;
    let mft_lcn = rd_u64(&vbr, 0x30);
    let cmft = vbr[0x40] as i8;
    let rec_size = if cmft < 0 {
        1usize << (-cmft as u32)
    } else {
        cmft as usize * spc * bps
    };
    let mft_off = mft_lcn * cluster;
    // total records via rec0 $DATA
    let rec0 = {
        let mut b = read_at(&mut r, mft_off, rec_size)?;
        apply_fixup(&mut b, bps);
        b
    };
    let mut total = 120000u64;
    {
        let mut ao = rd_u16(&rec0, 0x14) as usize;
        while ao + 16 <= rec0.len() {
            let at = rd_u32(&rec0, ao);
            if at == 0xFFFF_FFFF || at == 0 {
                break;
            }
            let al = rd_u32(&rec0, ao + 4) as usize;
            if al < 16 {
                break;
            }
            if at == 0x80 && rec0[ao + 8] != 0 {
                total = rd_u64(&rec0, ao + 0x30) / rec_size as u64;
            }
            ao += al;
        }
    }
    // Sentinel runs: recorded-but-unassigned stream runs (lcn_start == u64::MAX).
    // Their data IS in the stream; they just lack a real LCN. Clusters here are
    // recoverable if we can assign them; clusters NOT here were never even seen
    // by the scan (run header missed or Ghost didn't store them).
    let sentinel_runs = mapped.iter().filter(|&&(l, _)| l == u64::MAX).count();
    let sentinel_clusters: u64 = mapped
        .iter()
        .filter(|&&(l, _)| l == u64::MAX)
        .map(|&(_, c)| c)
        .sum();
    println!(
        "mapped ranges: {}  total MFT records: {}  cluster={}",
        mapped.len(),
        total,
        cluster
    );
    println!(
        "sentinel (recorded-but-unassigned) runs: {sentinel_runs}  clusters: {sentinel_clusters}"
    );

    let mut gap_clusters_by_rec: BTreeMap<u64, u64> = BTreeMap::new();
    let mut rec_name: BTreeMap<u64, String> = BTreeMap::new();
    let mut total_declared = 0u64;
    let mut total_gap = 0u64;

    for rec_num in 0..total {
        let off = mft_off + rec_num * rec_size as u64;
        let mut rec = match read_at(&mut r, off, rec_size) {
            Ok(b) => b,
            Err(_) => break,
        };
        if &rec[0..4] != b"FILE" {
            continue;
        }
        apply_fixup(&mut rec, bps);
        let flags = rd_u16(&rec, 22);
        if flags & 0x01 == 0 {
            continue;
        }
        let self_rec = rd_u32(&rec, 44) as u64;
        // Skip $MFT (0) and $BadClus (8): $MFT is served specially; $BadClus is a
        // whole-volume sparse run with no stored data.
        if self_rec == 0 || self_rec == 8 {
            continue;
        }
        let nm = file_name(&rec);

        let mut ao = rd_u16(&rec, 0x14) as usize;
        while ao + 16 <= rec.len() {
            let at = rd_u32(&rec, ao);
            if at == 0xFFFF_FFFF || at == 0 {
                break;
            }
            let al = rd_u32(&rec, ao + 4) as usize;
            if al < 16 || ao + al > rec.len() {
                break;
            }
            let nonres = rec[ao + 8] != 0;
            // Only $DATA (0x80), $INDEX_ALLOCATION (0xa0), $SECURITY_DESCRIPTOR
            // (0x50), $BITMAP (0xb0) carry real stored content worth auditing.
            if nonres && matches!(at, 0x80 | 0xa0 | 0x50 | 0xb0) {
                let run_off = rd_u16(&rec, ao + 0x20) as usize;
                if run_off > 0 && ao + run_off < ao + al {
                    for (lcn, len) in decode_runs(&rec[ao + run_off..ao + al]) {
                        for c in 0..len {
                            total_declared += 1;
                            if !covered(lcn + c) {
                                total_gap += 1;
                                *gap_clusters_by_rec.entry(self_rec).or_insert(0) += 1;
                                if let Some(n) = &nm {
                                    rec_name.entry(self_rec).or_insert_with(|| n.clone());
                                }
                            }
                        }
                    }
                }
            }
            ao += al;
        }
    }

    println!("\n=== GAP AUDIT ===");
    println!("declared content clusters: {total_declared}");
    println!("unmapped (read-as-zero) clusters: {total_gap}");
    println!("affected files (records): {}", gap_clusters_by_rec.len());
    println!("\nAll affected files by gap size:");
    let mut v: Vec<(u64, u64)> = gap_clusters_by_rec.iter().map(|(k, c)| (*k, *c)).collect();
    v.sort_by_key(|b| std::cmp::Reverse(b.1));
    for (rec, n) in v.iter().take(200) {
        let name = rec_name.get(rec).map(|s| s.as_str()).unwrap_or("?");
        println!("  rec#{rec} (0x{rec:X})  {n:>6} clusters  {name}");
    }
    Ok(())
}
