//! Definitive store-vs-omit check for $Bitmap. Parse MFT record 6 to find
//! $Bitmap's declared LCN run, then read the ENTIRE volume sequentially (which
//! forces the GHO reader's full lazy scan to complete) and capture exactly what
//! lands in $Bitmap's region. If it's all zeros after a complete scan, Ghost
//! did not store $Bitmap's content — it must be synthesized on restore.
//!
//! usage: gho_ntfs_bitmap <path-to.gho>

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

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: gho_ntfs_bitmap <path>");
    let p = std::path::Path::new(&path);

    // --- locate $Bitmap's run from MFT record 6 ---
    let mut r = rusty_backup::rbformats::gho::GhoReader::open(p).unwrap();
    r.seek(SeekFrom::Start(0)).unwrap();
    let vbr = rd(&mut r, 512);
    let cluster = (u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64 * vbr[0x0D] as u64) as usize;
    let total = u64::from_le_bytes(vbr[0x28..0x30].try_into().unwrap()) * 512;
    let mft_lcn = u64::from_le_bytes(vbr[0x30..0x38].try_into().unwrap());
    let cpr = vbr[0x40] as i8;
    let rec_size = if cpr >= 0 {
        cpr as u64 * cluster as u64
    } else {
        1u64 << (-cpr as u32)
    };
    r.seek(SeekFrom::Start(mft_lcn * cluster as u64 + 6 * rec_size))
        .unwrap();
    let rec = rd(&mut r, rec_size as usize);
    let mut off = u16::from_le_bytes([rec[0x14], rec[0x15]]) as usize;
    let (mut bm_lcn, mut bm_clusters) = (0u64, 0u64);
    while off + 8 <= rec.len() {
        let atype = u32::from_le_bytes(rec[off..off + 4].try_into().unwrap());
        if atype == 0xFFFF_FFFF {
            break;
        }
        let alen = u32::from_le_bytes(rec[off + 4..off + 8].try_into().unwrap()) as usize;
        if alen == 0 {
            break;
        }
        if atype == 0x80 && rec[off + 8] == 1 {
            let mpo = u16::from_le_bytes([rec[off + 0x20], rec[off + 0x21]]) as usize;
            let mut q = off + mpo;
            let hdr = rec[q];
            q += 1;
            let lb = (hdr & 0x0F) as usize;
            let ob = (hdr >> 4) as usize;
            for i in 0..lb {
                bm_clusters |= (rec[q + i] as u64) << (8 * i);
            }
            q += lb;
            let mut o = 0i64;
            for i in 0..ob {
                o |= (rec[q + i] as i64) << (8 * i);
            }
            bm_lcn = o as u64;
        }
        off += alen;
    }
    let bm_start = bm_lcn * cluster as u64;
    let bm_end = bm_start + bm_clusters * cluster as u64;
    println!("$Bitmap: lcn={bm_lcn} clusters={bm_clusters} (bytes {bm_start}..{bm_end})");

    // --- full sequential read; capture the $Bitmap region as it streams by ---
    let mut r2 = rusty_backup::rbformats::gho::GhoReader::open(p).unwrap();
    r2.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut pos = 0u64;
    let mut nonzero_in_bitmap = 0u64;
    let mut bytes_in_bitmap = 0u64;
    while pos < total {
        let want = ((total - pos) as usize).min(buf.len());
        let mut f = 0;
        while f < want {
            match r2.read(&mut buf[f..want]).unwrap() {
                0 => break,
                k => f += k,
            }
        }
        if f == 0 {
            break;
        }
        let chunk_start = pos;
        let chunk_end = pos + f as u64;
        if chunk_start < bm_end && chunk_end > bm_start {
            let s = bm_start.max(chunk_start);
            let e = bm_end.min(chunk_end);
            for i in s..e {
                bytes_in_bitmap += 1;
                if buf[(i - chunk_start) as usize] != 0 {
                    nonzero_in_bitmap += 1;
                }
            }
        }
        pos += f as u64;
    }
    println!(
        "after FULL sequential scan: $Bitmap region {bytes_in_bitmap} bytes, {nonzero_in_bitmap} nonzero"
    );
    if nonzero_in_bitmap == 0 {
        println!(">>> Ghost did NOT store $Bitmap content — OMITTED by design; must SYNTHESIZE.");
    } else {
        println!(">>> $Bitmap content present after full scan — MAPPING bug, not omission.");
    }
}
