// Probe MFT record 5 (the NTFS root directory) plus a handful of common
// metadata files via GhoReader, dumping each attribute's first bytes so we
// can tell which ones currently read wrong content.
//
// Usage:
//   cargo run --release --example gho_ntfs_probe_root -- PATH.GHO [REC ...]

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

fn read_at(r: &mut GhoReader, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut b = vec![0u8; len];
    r.seek(SeekFrom::Start(off))?;
    r.read_exact(&mut b)?;
    Ok(b)
}

fn type_name(t: u32) -> &'static str {
    match t {
        0x10 => "$STANDARD_INFORMATION",
        0x20 => "$ATTRIBUTE_LIST",
        0x30 => "$FILE_NAME",
        0x40 => "$OBJECT_ID",
        0x50 => "$SECURITY_DESCRIPTOR",
        0x60 => "$VOLUME_NAME",
        0x70 => "$VOLUME_INFORMATION",
        0x80 => "$DATA",
        0x90 => "$INDEX_ROOT",
        0xa0 => "$INDEX_ALLOCATION",
        0xb0 => "$BITMAP",
        0xc0 => "$REPARSE_POINT",
        0xd0 => "$EA_INFORMATION",
        0xe0 => "$EA",
        0x100 => "$LOGGED_UTILITY_STREAM",
        _ => "?",
    }
}

fn decode_runs(data: &[u8]) -> Vec<(i64, u64)> {
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

fn hex(b: &[u8], n: usize) -> String {
    b[..b.len().min(n)]
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("path to GHO");
    let recs: Vec<u64> = {
        let v: Vec<u64> = args.filter_map(|s| s.parse().ok()).collect();
        if v.is_empty() {
            vec![5, 9, 0, 2, 6, 8, 10]
        } else {
            v
        }
    };

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
    let mft_off = mft_lcn * cluster_size;

    for &rec_num in &recs {
        let off = mft_off + rec_num * rec_size as u64;
        let raw = read_at(&mut r, off, rec_size)?;
        let mut rec = raw.clone();
        if &rec[0..4] != b"FILE" {
            println!(
                "\n=== rec#{rec_num}: NOT A FILE RECORD (magic={}) ===",
                hex(&rec, 4)
            );
            continue;
        }
        apply_fixup(&mut rec, bps);
        let flags = rd_u16(&rec, 22);
        let used = rd_u32(&rec, 0x18);
        let alloc = rd_u32(&rec, 0x1c);
        println!("\n=== rec#{rec_num} (flags=0x{flags:04x}, used={used}, alloc={alloc}) ===");

        let mut ao = rd_u16(&rec, 0x14) as usize;
        let mut attr_idx = 0;
        while ao + 16 <= rec.len() {
            let at = rd_u32(&rec, ao);
            if at == 0xFFFF_FFFF || at == 0 {
                break;
            }
            let al = rd_u32(&rec, ao + 4) as usize;
            if al < 16 || ao + al > rec.len() {
                break;
            }
            let non_res = rec[ao + 8] != 0;
            let name_len = rec[ao + 9] as usize;
            let name_off = rd_u16(&rec, ao + 10) as usize;
            let name = if name_len > 0 && name_off > 0 {
                let mut s = String::new();
                for i in 0..name_len {
                    let c = rd_u16(&rec, ao + name_off + i * 2);
                    if let Some(ch) = char::from_u32(c as u32) {
                        s.push(ch);
                    }
                }
                s
            } else {
                String::new()
            };
            attr_idx += 1;
            if !non_res {
                let vl = rd_u32(&rec, ao + 0x10) as usize;
                let vo = rd_u16(&rec, ao + 0x14) as usize;
                let val = if ao + vo + vl <= rec.len() {
                    &rec[ao + vo..ao + vo + vl]
                } else {
                    &[]
                };
                println!(
                    "  attr#{attr_idx} type=0x{at:02x} {} '{}' RES vlen={vl} first16={}",
                    type_name(at),
                    name,
                    hex(val, 16)
                );
                // For $STANDARD_INFORMATION, dump security_id (offset 0x20 of value).
                if at == 0x10 && vl >= 0x48 {
                    let sec_id = u32::from_le_bytes([val[0x20], val[0x21], val[0x22], val[0x23]]);
                    println!("      security_id = {sec_id} (0x{sec_id:08x})");
                }
            } else {
                let start_vcn = rd_u64(&rec, ao + 0x10);
                let real = rd_u64(&rec, ao + 0x30);
                let run_off = rd_u16(&rec, ao + 0x20) as usize;
                let runs = if run_off > 0 && ao + run_off < ao + al {
                    decode_runs(&rec[ao + run_off..ao + al])
                } else {
                    Vec::new()
                };
                println!(
                    "  attr#{attr_idx} type=0x{at:02x} {} '{}' NONRES start_vcn={start_vcn} real={real}",
                    type_name(at), name
                );
                let mut vcn = start_vcn;
                for (i, (lcn, cc)) in runs.iter().enumerate() {
                    if *lcn <= 0 {
                        println!("    run#{i}: vcn={vcn}..{} SPARSE cc={cc}", vcn + cc);
                    } else {
                        let pos = *lcn as u64 * cluster_size;
                        let head = read_at(&mut r, pos, 32).unwrap_or_default();
                        println!(
                            "    run#{i}: vcn={vcn}..{} lcn={lcn} cc={cc} head={}",
                            vcn + cc,
                            hex(&head, 32)
                        );
                    }
                    vcn += cc;
                }
            }
            ao += al;
        }
    }
    Ok(())
}
