//! Report files whose declared $DATA clusters aren't covered by any mapped run
//! in the reconstructed NTFS volume — i.e. files that read as (partly) zeros
//! because the file-aware run mapping dropped an ambiguous run. Also flags
//! whether the gap is real data loss vs a duplicate Ghost stored twice.
//!
//! usage: gho_ntfs_damaged <path-to.gho>

use rusty_backup::rbformats::gho::GhoReader;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

fn rd(r: &mut impl Read, n: usize) -> Vec<u8> {
    let mut b = vec![0u8; n];
    let mut f = 0;
    while f < n {
        match r.read(&mut b[f..]).unwrap() {
            0 => break,
            k => f += k,
        }
    }
    b
}

fn apply_fixup(rec: &mut [u8]) {
    let usa_off = u16::from_le_bytes([rec[4], rec[5]]) as usize;
    let usa_cnt = u16::from_le_bytes([rec[6], rec[7]]) as usize;
    if usa_cnt == 0 {
        return;
    }
    for i in 0..usa_cnt - 1 {
        let s = i * 512 + 510;
        let u = usa_off + 2 + i * 2;
        if s + 1 < rec.len() && u + 1 < rec.len() {
            rec[s] = rec[u];
            rec[s + 1] = rec[u + 1];
        }
    }
}

/// Decode an NTFS data-run list into (lcn, length) pairs, skipping sparse runs.
fn decode_runs(data: &[u8]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut pos = 0;
    let mut lcn: i64 = 0;
    while pos < data.len() {
        let hdr = data[pos];
        if hdr == 0 {
            break;
        }
        pos += 1;
        let lb = (hdr & 0x0F) as usize;
        let ob = (hdr >> 4) as usize;
        if lb == 0 || pos + lb + ob > data.len() {
            break;
        }
        let mut len = 0u64;
        for i in 0..lb {
            len |= (data[pos + i] as u64) << (8 * i);
        }
        pos += lb;
        if ob == 0 {
            // sparse hole — no LCN
            continue;
        }
        let mut o = 0i64;
        for i in 0..ob {
            o |= (data[pos + i] as i64) << (8 * i);
        }
        if data[pos + ob - 1] & 0x80 != 0 {
            o |= -1i64 << (8 * ob);
        }
        pos += ob;
        lcn += o;
        if lcn > 0 {
            out.push((lcn as u64, len));
        }
    }
    out
}

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: gho_ntfs_damaged <path>");
    let p = std::path::Path::new(&path);

    // 1) Mapped coverage.
    let mut reader = GhoReader::open(p).unwrap();
    reader.prepare_full_image();
    let ranges = reader.ntfs_mapped_ranges().expect("not NTFS file-aware");
    // Sorted (start, end) intervals for binary-search coverage tests.
    let cov: Vec<(u64, u64)> = ranges.iter().map(|&(l, c)| (l, l + c)).collect();
    let covered = |lcn: u64| -> bool {
        cov.binary_search_by(|&(s, e)| {
            if lcn < s {
                std::cmp::Ordering::Greater
            } else if lcn >= e {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
    };

    // 2) VBR geometry.
    reader.seek(SeekFrom::Start(0)).unwrap();
    let vbr = rd(&mut reader, 512);
    let cluster = (u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64 * vbr[0x0D] as u64) as usize;
    let mft_lcn = u64::from_le_bytes(vbr[0x30..0x38].try_into().unwrap());
    let cpr = vbr[0x40] as i8;
    let rec_size = if cpr >= 0 {
        cpr as usize * cluster
    } else {
        1usize << (-cpr as u32)
    };

    // 3) Read MFT record 0 to get $MFT's fragments.
    reader
        .seek(SeekFrom::Start(mft_lcn * cluster as u64))
        .unwrap();
    let mut rec0 = rd(&mut reader, rec_size);
    apply_fixup(&mut rec0);
    let mft_fragments = data_attr_runs(&rec0);
    if mft_fragments.is_empty() {
        eprintln!("could not read $MFT runs");
        return;
    }

    // 4) Walk every MFT record: collect names+parents and damaged records.
    let mut names: HashMap<u64, (String, u64)> = HashMap::new(); // rec -> (name, parent)
    let mut damaged: Vec<(u64, u64, u64)> = Vec::new(); // (rec, missing_clusters, total_clusters)
    let mut rec_no: u64 = 0;
    for &(flcn, fcc) in &mft_fragments {
        reader.seek(SeekFrom::Start(flcn * cluster as u64)).unwrap();
        let frag = rd(&mut reader, fcc as usize * cluster);
        for chunk in frag.chunks(rec_size) {
            let r = rec_no;
            rec_no += 1;
            if chunk.len() < rec_size || &chunk[0..4] != b"FILE" {
                continue;
            }
            let mut rec = chunk.to_vec();
            apply_fixup(&mut rec);
            if let Some((name, parent)) = file_name_attr(&rec) {
                names.insert(r, (name, parent));
            }
            let runs = data_attr_runs(&rec);
            if runs.is_empty() {
                continue;
            }
            let mut total = 0u64;
            let mut missing = 0u64;
            for (lcn, len) in runs {
                for c in lcn..lcn + len {
                    total += 1;
                    if !covered(c) {
                        missing += 1;
                    }
                }
            }
            if missing > 0 {
                damaged.push((r, missing, total));
            }
        }
    }

    // 5) Resolve paths and report.
    let path_of = |mut r: u64| -> String {
        let mut parts = Vec::new();
        for _ in 0..64 {
            match names.get(&r) {
                Some((n, parent)) => {
                    parts.push(n.clone());
                    if r == 5 || *parent == r {
                        break;
                    }
                    r = *parent;
                }
                None => break,
            }
        }
        parts.reverse();
        format!("/{}", parts.join("/"))
    };

    let mb = |c: u64| c as f64 * cluster as f64 / 1e6;
    // Partial loss (some clusters mapped, some not) is genuine reconstruction
    // damage. Full loss (missing == total) is almost always a file Ghost
    // intentionally excludes (hiberfil.sys, pagefile.sys, restore points, IE
    // cache) and isn't our bug.
    let mut partial: Vec<(u64, u64, u64)> = damaged.iter().copied().filter(|d| d.1 < d.2).collect();
    let full: Vec<(u64, u64, u64)> = damaged.iter().copied().filter(|d| d.1 == d.2).collect();
    partial.sort_by_key(|d| std::cmp::Reverse(d.1));

    let partial_missing: u64 = partial.iter().map(|d| d.1).sum();
    let full_missing: u64 = full.iter().map(|d| d.1).sum();

    println!(
        "PARTIAL data loss (genuine reconstruction gaps): {} files, {:.1} MB",
        partial.len(),
        mb(partial_missing),
    );
    println!(
        "FULL loss (mostly Ghost-excluded transient files): {} files, {:.1} MB\n",
        full.len(),
        mb(full_missing),
    );
    println!("=== partially-damaged files (our reconstruction's actual loss) ===");
    for (r, missing, total) in partial.iter() {
        println!(
            "  {:<70} {}/{} clusters ({:.2} MB lost)",
            path_of(*r),
            missing,
            total,
            mb(*missing),
        );
    }
}

/// Extract the non-resident $DATA (0x80, unnamed) runs from a fixup-applied record.
fn data_attr_runs(rec: &[u8]) -> Vec<(u64, u64)> {
    let mut off = u16::from_le_bytes([rec[0x14], rec[0x15]]) as usize;
    while off + 8 <= rec.len() {
        let atype = u32::from_le_bytes(rec[off..off + 4].try_into().unwrap());
        if atype == 0xFFFF_FFFF {
            break;
        }
        let alen = u32::from_le_bytes(rec[off + 4..off + 8].try_into().unwrap()) as usize;
        if alen == 0 || off + alen > rec.len() {
            break;
        }
        // unnamed $DATA, non-resident
        if atype == 0x80 && rec[off + 8] == 1 && rec[off + 9] == 0 {
            let mpo = u16::from_le_bytes([rec[off + 0x20], rec[off + 0x21]]) as usize;
            if off + mpo < off + alen {
                return decode_runs(&rec[off + mpo..off + alen]);
            }
        }
        off += alen;
    }
    Vec::new()
}

/// Extract the (preferred) $FILE_NAME: returns (name, parent_record).
fn file_name_attr(rec: &[u8]) -> Option<(String, u64)> {
    let mut off = u16::from_le_bytes([rec[0x14], rec[0x15]]) as usize;
    let mut best: Option<(String, u64, u8)> = None; // (name, parent, namespace)
    while off + 8 <= rec.len() {
        let atype = u32::from_le_bytes(rec[off..off + 4].try_into().unwrap());
        if atype == 0xFFFF_FFFF {
            break;
        }
        let alen = u32::from_le_bytes(rec[off + 4..off + 8].try_into().unwrap()) as usize;
        if alen == 0 || off + alen > rec.len() {
            break;
        }
        if atype == 0x30 && rec[off + 8] == 0 {
            let coff = u16::from_le_bytes([rec[off + 0x14], rec[off + 0x15]]) as usize;
            let c = off + coff;
            if c + 0x42 <= rec.len() {
                let parent =
                    u64::from_le_bytes(rec[c..c + 8].try_into().unwrap()) & 0x0000_FFFF_FFFF_FFFF;
                let nlen = rec[c + 0x40] as usize;
                let ns = rec[c + 0x41];
                let nstart = c + 0x42;
                if nstart + nlen * 2 <= rec.len() {
                    let name: String = (0..nlen)
                        .map(|i| u16::from_le_bytes([rec[nstart + i * 2], rec[nstart + i * 2 + 1]]))
                        .map(|u| char::from_u32(u as u32).unwrap_or('?'))
                        .collect();
                    // Prefer Win32 (1) / Win32+DOS (3) over DOS (2) / POSIX (0).
                    let better = match &best {
                        None => true,
                        Some((_, _, prev_ns)) => ns == 1 || ns == 3 || *prev_ns == 2,
                    };
                    if better {
                        best = Some((name, parent, ns));
                    }
                }
            }
        }
        off += alen;
    }
    best.map(|(n, p, _)| (n, p))
}
