// Diagnostic: inspect the $MFT layout of a GHO/Ghost NTFS image to detect
// MFT fragmentation (the cause of "MFT record N has invalid magic [0,0,0,0]").
//
//   cargo run --example gho_mft_fragments -- /path/to/image.gho [record_number]
//
// Reads MFT record 0 ($MFT) through the GHO logical reader, decodes its
// $DATA data runs, and reports each fragment. If a target record number is
// given, it reports which fragment that record falls into.

use std::env;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use rusty_backup::rbformats::gho::GhoReader;

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut args = env::args().skip(1);
    let path = PathBuf::from(
        args.next()
            .expect("usage: gho_mft_fragments <path.gho> [record]"),
    );
    let target_record: Option<u64> = args.next().and_then(|s| s.parse().ok());

    let mut r = GhoReader::open(&path)?;

    // --- VBR ---
    let mut vbr = [0u8; 512];
    r.seek(SeekFrom::Start(0))?;
    r.read_exact(&mut vbr)?;
    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    let sectors_per_cluster = vbr[0x0D] as u64;
    let cluster_size = bytes_per_sector * sectors_per_cluster;
    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);
    let cpm = vbr[0x40] as i8;
    let mft_record_size: u64 = if cpm < 0 {
        1u64 << (-cpm as u32)
    } else {
        cpm as u64 * cluster_size
    };
    println!(
        "cluster_size={cluster_size}  mft_cluster={mft_cluster}  mft_record_size={mft_record_size}"
    );

    // --- read $MFT record 0 (always in the first fragment, at mft_cluster) ---
    let mut rec = vec![0u8; mft_record_size as usize];
    r.seek(SeekFrom::Start(mft_cluster * cluster_size))?;
    r.read_exact(&mut rec)?;
    apply_fixup(&mut rec, bytes_per_sector as usize);
    if &rec[0..4] != b"FILE" {
        anyhow::bail!(
            "record 0 magic is {:?} — could not read $MFT itself",
            &rec[0..4]
        );
    }

    let runs = data_runs_of_attr(&rec, 0x80)
        .ok_or_else(|| anyhow::anyhow!("no non-resident $DATA on $MFT"))?;
    println!("\n$MFT has {} data-run fragment(s):", runs.len());
    let mut vcn = 0u64;
    let mut total_clusters = 0u64;
    for (i, (lcn, len)) in runs.iter().enumerate() {
        let rec_lo = vcn * cluster_size / mft_record_size;
        let rec_hi = (vcn + len) * cluster_size / mft_record_size;
        println!(
            "  frag {i}: lcn={lcn:>10}  clusters={len:>8}  -> MFT records [{rec_lo}..{rec_hi})",
        );
        vcn += len;
        total_clusters += len;
    }
    let total_records = total_clusters * cluster_size / mft_record_size;
    println!("total MFT clusters={total_clusters}  -> {total_records} records");
    if runs.len() > 1 {
        println!("\n>> MFT IS FRAGMENTED. The GHO reader only maps the first fragment,");
        println!(
            ">> so records beyond record {} read back as zeros.",
            runs[0].1 * cluster_size / mft_record_size
        );
    } else {
        println!("\n>> MFT is contiguous — fragmentation is not the cause here.");
    }

    if let Some(n) = target_record {
        let byte = n * mft_record_size;
        let vcn_n = byte / cluster_size;
        let mut acc = 0u64;
        let mut found = None;
        for (i, (_lcn, len)) in runs.iter().enumerate() {
            if vcn_n >= acc && vcn_n < acc + len {
                found = Some(i);
                break;
            }
            acc += len;
        }
        match found {
            Some(0) => println!("\nrecord {n} -> fragment 0 (mapped today, should read fine)"),
            Some(i) => println!("\nrecord {n} -> fragment {i} (NOT mapped by current GHO reader)"),
            None => {
                println!("\nrecord {n} -> beyond all $MFT fragments (record number out of range)")
            }
        }
    }

    Ok(())
}

/// Apply NTFS update-sequence fixup in place.
fn apply_fixup(rec: &mut [u8], _bps: usize) {
    let usa_off = u16::from_le_bytes([rec[4], rec[5]]) as usize;
    let usa_count = u16::from_le_bytes([rec[6], rec[7]]) as usize;
    if usa_count == 0 || usa_off + usa_count * 2 > rec.len() {
        return;
    }
    let usn = [rec[usa_off], rec[usa_off + 1]];
    for i in 1..usa_count {
        let sector_end = i * 512;
        if sector_end > rec.len() {
            break;
        }
        let entry = usa_off + i * 2;
        let _check = [rec[sector_end - 2], rec[sector_end - 1]];
        rec[sector_end - 2] = rec[entry];
        rec[sector_end - 1] = rec[entry + 1];
        let _ = usn;
    }
}

/// Decode the data runs of the first non-resident attribute of `attr_type`.
/// Returns Vec<(absolute_lcn, length_in_clusters)>.
fn data_runs_of_attr(rec: &[u8], attr_type: u32) -> Option<Vec<(u64, u64)>> {
    let mut pos = u16::from_le_bytes([rec[0x14], rec[0x15]]) as usize;
    while pos + 8 <= rec.len() {
        let atype = u32::from_le_bytes([rec[pos], rec[pos + 1], rec[pos + 2], rec[pos + 3]]);
        if atype == 0xFFFF_FFFF {
            break;
        }
        let alen =
            u32::from_le_bytes([rec[pos + 4], rec[pos + 5], rec[pos + 6], rec[pos + 7]]) as usize;
        if alen == 0 {
            break;
        }
        let non_resident = rec[pos + 8] != 0;
        if atype == attr_type && non_resident {
            let mpo = u16::from_le_bytes([rec[pos + 0x20], rec[pos + 0x21]]) as usize;
            return Some(decode_runs(&rec[pos + mpo..pos + alen]));
        }
        pos += alen;
    }
    None
}

fn decode_runs(buf: &[u8]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut lcn: i64 = 0;
    while i < buf.len() {
        let header = buf[i];
        if header == 0 {
            break;
        }
        let len_bytes = (header & 0x0F) as usize;
        let off_bytes = ((header >> 4) & 0x0F) as usize;
        i += 1;
        if i + len_bytes + off_bytes > buf.len() {
            break;
        }
        let mut length: u64 = 0;
        for b in 0..len_bytes {
            length |= (buf[i + b] as u64) << (b * 8);
        }
        i += len_bytes;
        let mut delta: i64 = 0;
        for b in 0..off_bytes {
            delta |= (buf[i + b] as i64) << (b * 8);
        }
        if off_bytes > 0 && buf[i + off_bytes - 1] & 0x80 != 0 {
            delta |= -1i64 << (off_bytes * 8); // sign-extend
        }
        i += off_bytes;
        lcn += delta;
        out.push((lcn as u64, length));
    }
    out
}
