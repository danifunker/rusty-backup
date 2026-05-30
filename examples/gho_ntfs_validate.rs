// Validate the reconstructed NTFS volume produced by the real GhoReader path.
// Checks:
//   1. $Secure (rec 9): its $SDH and $SII $INDEX_ALLOCATION blocks land at the
//      right LCNs (identified by first index entry key_length 8 vs 4).
//   2. Every $INDEX_ALLOCATION block has "INDX" magic (no zero/garbage blocks).
//   3. For each $INDEX_ALLOCATION attribute, each block's self-VCN (offset 0x10)
//      equals its expected VCN -> catches within-attribute block swaps.
use rusty_backup::rbformats::gho::GhoReader;
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
fn decode_data_runs(data: &[u8]) -> Vec<(i64, u64)> {
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
            out.push((lcn, length));
        } else {
            out.push((0, length));
        }
    }
    out
}

fn read_at(r: &mut GhoReader, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut b = vec![0u8; len];
    r.seek(SeekFrom::Start(off))?;
    r.read_exact(&mut b)?;
    Ok(b)
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let path = std::env::args().nth(1).expect("path to GHO");
    let mut r = GhoReader::open(std::path::Path::new(&path))?;
    r.prepare_full_image();

    let vbr = read_at(&mut r, 0, 512)?;
    let bps = rd_u16(&vbr, 0x0B) as usize;
    let spc = vbr[0x0D] as usize;
    let cluster_size = (bps * spc) as u64;
    let mft_lcn = rd_u64(&vbr, 0x30);
    let cmft = vbr[0x40] as i8;
    let rec_size = if cmft < 0 {
        1usize << (-cmft as u32)
    } else {
        cmft as usize * spc * bps
    };
    let idx_rec_size = {
        // bytes-or-clusters per index record at VBR 0x44 (signed)
        let v = vbr[0x44] as i8;
        if v < 0 {
            1usize << (-v as u32)
        } else {
            v as usize * cluster_size as usize
        }
    };
    println!(
        "cluster_size={cluster_size} rec_size={rec_size} idx_rec_size={idx_rec_size} mft_lcn={mft_lcn}"
    );

    let mft_off = mft_lcn * cluster_size;
    // Determine MFT size by reading record 0's $DATA run lengths.
    let rec0 = read_at(&mut r, mft_off, rec_size)?;
    let mut rec0m = rec0.clone();
    apply_fixup(&mut rec0m, bps);
    // Walk a generous number of records by stepping until non-FILE for a while.
    // Use $MFT $DATA total to size; fall back to scanning 200k records.
    let mut total_records = 0u64;
    {
        let mut off = rd_u16(&rec0m, 20) as usize;
        while off + 8 <= rec0m.len() {
            let at = rd_u32(&rec0m, off);
            if at == 0xFFFF_FFFF {
                break;
            }
            let al = rd_u32(&rec0m, off + 4) as usize;
            if al == 0 || off + al > rec0m.len() {
                break;
            }
            if at == 0x80 && rec0m[off + 8] != 0 {
                let ro = rd_u16(&rec0m, off + 32) as usize;
                let runs = decode_data_runs(&rec0m[off + ro..off + al]);
                let clusters: u64 = runs.iter().map(|(_, c)| c).sum();
                total_records = clusters * cluster_size / rec_size as u64;
            }
            off += al;
        }
    }
    if total_records == 0 {
        total_records = 90000;
    }
    println!("MFT total_records ~= {total_records}");

    let mut indx_blocks = 0u64;
    let mut indx_bad_magic = 0u64;
    let mut free_zero_blocks = 0u64;
    let mut vcn_mismatch = 0u64;
    let mut sdh_lcn = None;
    let mut sii_lcn = None;
    let mut sdh_ok = false;
    let mut sii_ok = false;

    for recn in 0..total_records {
        let off = mft_off + recn * rec_size as u64;
        let mut rec = match read_at(&mut r, off, rec_size) {
            Ok(b) => b,
            Err(_) => break,
        };
        if &rec[0..4] != b"FILE" {
            continue;
        }
        let rec_num = rd_u32(&rec, 44);
        let flags = rd_u16(&rec, 22);
        if flags & 0x01 == 0 {
            continue; // not in use (deleted) — stale attrs, skip
        }
        apply_fixup(&mut rec, bps);
        // First pass: grab the $I30 $BITMAP (type 0xb0, resident) — bit v set =
        // index buffer VCN v is in use. Free buffers legitimately read as zeros.
        let mut idx_bitmap: Vec<u8> = Vec::new();
        {
            let mut ao = rd_u16(&rec, 20) as usize;
            while ao + 8 <= rec.len() {
                let at = rd_u32(&rec, ao);
                if at == 0xFFFF_FFFF {
                    break;
                }
                let al = rd_u32(&rec, ao + 4) as usize;
                if al == 0 || ao + al > rec.len() {
                    break;
                }
                if at == 0xb0 && rec[ao + 8] == 0 {
                    let voff = rd_u16(&rec, ao + 20) as usize;
                    let vlen = rd_u32(&rec, ao + 16) as usize;
                    if ao + voff + vlen <= rec.len() {
                        idx_bitmap = rec[ao + voff..ao + voff + vlen].to_vec();
                    }
                }
                ao += al;
            }
        }
        let vcn_in_use = |v: u64| -> bool {
            if idx_bitmap.is_empty() {
                return true; // no bitmap parsed -> assume in use (conservative)
            }
            let byte = (v / 8) as usize;
            idx_bitmap
                .get(byte)
                .map(|b| b & (1 << (v % 8)) != 0)
                .unwrap_or(false)
        };
        // Walk attributes; for each non-resident $INDEX_ALLOCATION (0xa0):
        let mut ao = rd_u16(&rec, 20) as usize;
        while ao + 8 <= rec.len() {
            let at = rd_u32(&rec, ao);
            if at == 0xFFFF_FFFF {
                break;
            }
            let al = rd_u32(&rec, ao + 4) as usize;
            if al == 0 || ao + al > rec.len() {
                break;
            }
            if at == 0xa0 && rec[ao + 8] != 0 {
                let ro = rd_u16(&rec, ao + 32) as usize;
                let runs = decode_data_runs(&rec[ao + ro..ao + al]);
                let per_run_blocks = (cluster_size as usize / idx_rec_size).max(1);
                let blocks_per_cluster = per_run_blocks; // if idx<=cluster
                let _ = blocks_per_cluster;
                let mut expected_vcn = 0u64;
                for (lcn, cc) in &runs {
                    if *lcn <= 0 {
                        expected_vcn += cc * cluster_size / idx_rec_size as u64;
                        continue;
                    }
                    let bytes = cc * cluster_size;
                    let nblocks = bytes / idx_rec_size as u64;
                    let base = *lcn as u64 * cluster_size;
                    for bi in 0..nblocks {
                        let bo = base + bi * idx_rec_size as u64;
                        let blk = match read_at(&mut r, bo, idx_rec_size) {
                            Ok(b) => b,
                            Err(_) => break,
                        };
                        indx_blocks += 1;
                        if &blk[0..4] != b"INDX" {
                            let in_use = vcn_in_use(expected_vcn);
                            if in_use {
                                indx_bad_magic += 1;
                                if indx_bad_magic <= 15 {
                                    println!(
                                        "  BAD-MAGIC(in-use) rec#{rec_num} vcn={expected_vcn} \
                                         lcn={lcn} magic={:02x?} (run cc={cc})",
                                        &blk[0..4]
                                    );
                                }
                            } else {
                                free_zero_blocks += 1;
                            }
                            expected_vcn += 1;
                            continue;
                        }
                        let self_vcn = rd_u64(&blk, 0x10);
                        if self_vcn != expected_vcn {
                            vcn_mismatch += 1;
                            if vcn_mismatch <= 20 {
                                println!(
                                    "  VCN-MISMATCH rec#{rec_num} expected_vcn={expected_vcn} \
                                     self_vcn={self_vcn} lcn={lcn} cc={cc}"
                                );
                            }
                        }
                        // $Secure id: capture $SDH/$SII keylen
                        if rec_num == 9 {
                            let node = 0x18usize;
                            let first = rd_u32(&blk, node) as usize + node;
                            if first + 0x0C <= blk.len() {
                                let kl = rd_u16(&blk, first + 0x0A);
                                if kl == 8 {
                                    sdh_lcn = Some(*lcn as u64);
                                    sdh_ok = true;
                                } else if kl == 4 {
                                    sii_lcn = Some(*lcn as u64);
                                    sii_ok = true;
                                }
                            }
                        }
                        expected_vcn += 1;
                    }
                }
            }
            ao += al;
        }
    }

    println!("\n--- $Secure (file 9) index allocation ---");
    println!(
        "  $SDH block found (keylen=8): {sdh_ok} at lcn={:?}",
        sdh_lcn
    );
    println!(
        "  $SII block found (keylen=4): {sii_ok} at lcn={:?}",
        sii_lcn
    );
    // The MFT says $SDH is the first 0xa0 run (lower lcn), $SII second.
    if let (Some(sdh), Some(sii)) = (sdh_lcn, sii_lcn) {
        if sdh < sii {
            println!("  ORDER OK: $SDH lcn < $SII lcn (matches MFT attribute order)");
        } else {
            println!("  ORDER WRONG: $SDH lcn ({sdh}) >= $SII lcn ({sii}) -> SWAPPED");
        }
    }
    println!("\n--- index allocation totals ---");
    println!("  INDX blocks read: {indx_blocks}");
    println!("  bad magic (in-use VCN, real corruption): {indx_bad_magic}");
    println!("  zero blocks for FREE VCNs (benign): {free_zero_blocks}");
    println!("  self-VCN mismatches: {vcn_mismatch}");
    Ok(())
}
