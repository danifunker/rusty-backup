// Find MFT records whose $FILE_NAME matches a target name, dump their base
// attributes, and (if present) parse $ATTRIBUTE_LIST to report which extension
// records hold each attribute. Then read those extension records and report
// whether they're readable (valid FILE magic) or zeros/garbage.
//
// Usage:
//   cargo run --release --example gho_find_attrlist -- PATH.GHO name1 [name2 ...]
// Defaults to looking for "system" and "software".

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

// Extract the $FILE_NAME (namespace-agnostic, first one) from a record.
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
            // resident $FILE_NAME
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
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("path to GHO");
    let names: Vec<String> = {
        let v: Vec<String> = args.map(|s| s.to_lowercase()).collect();
        if v.is_empty() {
            vec!["system".into(), "software".into()]
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
    // Discover total MFT records via rec 0's $DATA.
    let rec0 = {
        let mut b = read_at(&mut r, mft_off, rec_size)?;
        apply_fixup(&mut b, bps);
        b
    };
    let mut total_records = 120000u64;
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
                let real = rd_u64(&rec0, ao + 0x30);
                total_records = real / rec_size as u64;
            }
            ao += al;
        }
    }
    println!("scanning {total_records} MFT records for names: {names:?}");

    for rec_num in 0..total_records {
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
        let Some(nm) = file_name(&rec) else { continue };
        if !names.contains(&nm.to_lowercase()) {
            continue;
        }
        let size_hint = {
            // $DATA real size if present in base
            let mut ao = rd_u16(&rec, 0x14) as usize;
            let mut sz = 0u64;
            while ao + 16 <= rec.len() {
                let at = rd_u32(&rec, ao);
                if at == 0xFFFF_FFFF || at == 0 {
                    break;
                }
                let al = rd_u32(&rec, ao + 4) as usize;
                if al < 16 {
                    break;
                }
                if at == 0x80 && rec[ao + 8] != 0 {
                    sz = rd_u64(&rec, ao + 0x30);
                }
                ao += al;
            }
            sz
        };
        println!(
            "\n=== '{nm}' at MFT rec#{rec_num} (0x{rec_num:X}) base-$DATA-size={size_hint} ==="
        );

        // Walk attributes; note presence of $DATA and $ATTRIBUTE_LIST.
        let mut has_data = false;
        let mut attrlist: Option<(bool, u64, u32, usize)> = None; // (nonres, lcn|valoff, len, attrlen)
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
            match at {
                0x80 => {
                    has_data = true;
                    println!("  base has $DATA (nonres={nonres})");
                }
                0x20 => {
                    if !nonres {
                        let vo = rd_u16(&rec, ao + 0x14) as usize;
                        let vl = rd_u32(&rec, ao + 0x10);
                        attrlist = Some((false, (ao + vo) as u64, vl, al));
                    } else {
                        attrlist = Some((true, 0, 0, al));
                    }
                    println!("  base has $ATTRIBUTE_LIST (nonres={nonres})");
                }
                _ => {}
            }
            ao += al;
        }

        if !has_data {
            println!("  -> NO $DATA in base record (lives in extension via $ATTRIBUTE_LIST)");
        }

        // Parse resident $ATTRIBUTE_LIST entries to find which records hold $DATA.
        if let Some((false, valoff, vl, _)) = attrlist {
            let start = valoff as usize;
            let end = (start + vl as usize).min(rec.len());
            let mut p = start;
            let mut refs: Vec<(u32, u64)> = Vec::new(); // (attr_type, mft_ref)
            while p + 0x1A <= end {
                let atype = rd_u32(&rec, p);
                let entry_len = rd_u16(&rec, p + 4) as usize;
                if entry_len < 0x1A || p + entry_len > end {
                    break;
                }
                let mref = rd_u64(&rec, p + 0x10) & 0x0000_FFFF_FFFF_FFFF;
                refs.push((atype, mref));
                p += entry_len;
            }
            // Report the $DATA (0x80) referenced records.
            let data_recs: Vec<u64> = refs
                .iter()
                .filter(|(t, _)| *t == 0x80)
                .map(|(_, m)| *m)
                .collect();
            println!(
                "  $ATTRIBUTE_LIST entries: {} total; $DATA in records: {:?}",
                refs.len(),
                {
                    let mut d = data_recs.clone();
                    d.sort();
                    d.dedup();
                    d.iter()
                        .map(|n| format!("{n}(0x{n:X})"))
                        .collect::<Vec<_>>()
                }
            );
            // Check readability of each referenced extension record.
            let mut all: Vec<u64> = refs.iter().map(|(_, m)| *m).collect();
            all.sort();
            all.dedup();
            for er in all {
                if er == rec_num {
                    continue;
                }
                let eoff = mft_off + er * rec_size as u64;
                let eb = read_at(&mut r, eoff, rec_size).unwrap_or_default();
                let magic = if eb.len() >= 4 {
                    if &eb[0..4] == b"FILE" {
                        "FILE".to_string()
                    } else if eb.iter().take(64).all(|&x| x == 0) {
                        "ZEROS".to_string()
                    } else {
                        format!(
                            "garbage[{:02x} {:02x} {:02x} {:02x}]",
                            eb[0], eb[1], eb[2], eb[3]
                        )
                    }
                } else {
                    "short".into()
                };
                let marker = if magic == "FILE" { "ok" } else { "<<< BROKEN" };
                println!("    extension rec#{er}(0x{er:X}): {magic} {marker}");
            }
        }
    }
    Ok(())
}
