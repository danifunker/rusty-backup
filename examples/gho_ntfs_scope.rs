//! Scope check: for each critical NTFS system file (MFT records 0-15), read its
//! non-resident $DATA / $INDEX_ALLOCATION runs from the reconstructed whole-disk
//! GHO stream and report how many of its clusters read back as zeros. Zeroed
//! metadata = the whole-disk image is not a valid NTFS volume.
//!
//! usage: gho_ntfs_scope <path-to.gho>

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

/// Apply NTFS update-sequence (fixup) array to a record in place.
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

const NAMES: [&str; 16] = [
    "$MFT", "$MFTMirr", "$LogFile", "$Volume", "$AttrDef", ". (root)", "$Bitmap", "$Boot",
    "$BadClus", "$Secure", "$UpCase", "$Extend", "(12)", "(13)", "(14)", "(15)",
];

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: gho_ntfs_scope <path>");
    let mut r = rusty_backup::rbformats::gho::GhoReader::open(std::path::Path::new(&path)).unwrap();

    r.seek(SeekFrom::Start(0)).unwrap();
    let vbr = rd(&mut r, 512);
    let cluster = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64 * vbr[0x0D] as u64;
    let mft_lcn = u64::from_le_bytes(vbr[0x30..0x38].try_into().unwrap());
    let cpr = vbr[0x40] as i8;
    let rec_size = if cpr >= 0 {
        cpr as u64 * cluster
    } else {
        1u64 << (-cpr as u32)
    };

    println!(
        "{:<10} {:>6} {:>10} {:>10}  verdict",
        "file", "attr", "clusters", "zeroed"
    );
    for recno in 0u64..16 {
        let rec_off = mft_lcn * cluster + recno * rec_size;
        r.seek(SeekFrom::Start(rec_off)).unwrap();
        let mut rec = rd(&mut r, rec_size as usize);
        if &rec[0..4] != b"FILE" {
            continue;
        }
        apply_fixup(&mut rec);
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
            // $DATA (0x80) or $INDEX_ALLOCATION (0xA0), non-resident
            if (atype == 0x80 || atype == 0xA0) && rec[off + 8] == 1 {
                let mpo = u16::from_le_bytes([rec[off + 0x20], rec[off + 0x21]]) as usize;
                let mut p = off + mpo;
                let mut lcn: i64 = 0;
                let mut total = 0u64;
                let mut zeroed = 0u64;
                let mut sampled = 0u64;
                while p < off + alen {
                    let hdr = rec[p];
                    if hdr == 0 {
                        break;
                    }
                    p += 1;
                    let lb = (hdr & 0x0F) as usize;
                    let ob = (hdr >> 4) as usize;
                    let mut len = 0u64;
                    for i in 0..lb {
                        len |= (rec[p + i] as u64) << (8 * i);
                    }
                    p += lb;
                    let mut o = 0i64;
                    for i in 0..ob {
                        o |= (rec[p + i] as i64) << (8 * i);
                    }
                    if ob > 0 && rec[p + ob - 1] & 0x80 != 0 {
                        o |= -1i64 << (8 * ob);
                    }
                    p += ob;
                    lcn += o;
                    total += len;
                    // sample up to 8 clusters across this run
                    let step = (len / 8).max(1);
                    let mut c = 0u64;
                    while c < len {
                        r.seek(SeekFrom::Start((lcn as u64 + c) * cluster)).unwrap();
                        let buf = rd(&mut r, cluster as usize);
                        if buf.iter().all(|&b| b == 0) {
                            zeroed += 1;
                        }
                        sampled += 1;
                        c += step;
                    }
                }
                let aname = if atype == 0x80 { "$DATA" } else { "$I30" };
                let verdict = if zeroed == sampled {
                    "ALL ZEROS *** broken"
                } else if zeroed > 0 {
                    "partial zeros *"
                } else {
                    "present OK"
                };
                println!(
                    "{:<10} {:>6} {:>10} {:>4}/{:<5}  {}",
                    NAMES[recno as usize], aname, total, zeroed, sampled, verdict
                );
            }
            off += alen;
        }
    }
}
