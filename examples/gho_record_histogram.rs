//! Histogram of record types across the inner record stream of a
//! file-aware GHO. Used to discover whether NTFS introduces record
//! types beyond what we saw for FAT (0x0017, 0x0004, 0x0102, 0x0103,
//! 0x0104, 0x0002).
//!
//! Run: `cargo run --release --example gho_record_histogram -- <path>`

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use rusty_backup::rbformats::gho::{
    find_inner_stream_start, GhoContainerHeader, GHO_HEADER_PREFIX_LEN, GHO_PASSWORD_VERIFIER_LEN,
    GHO_RECORD_HEADER_LEN, GHO_RECORD_MAGIC,
};

fn main() {
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: gho_record_histogram <path>"),
    );
    let max_records: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    let mut f = File::open(&path).unwrap();
    let header = GhoContainerHeader::parse(&mut f).unwrap();
    println!(
        "file={} compression={:?} image_type={:?} password={}",
        path.display(),
        header.compression,
        header.image_type,
        header.password_protected
    );
    let header_end = (GHO_HEADER_PREFIX_LEN
        + if header.password_protected {
            GHO_PASSWORD_VERIFIER_LEN
        } else {
            0
        }) as u64;
    let inner_start = find_inner_stream_start(&mut f, header_end).unwrap();
    println!("inner stream @ {:#x}", inner_start);

    let mut hist: BTreeMap<u16, (u64, u64, u32, u32)> = BTreeMap::new(); // type -> (count, total_body_bytes, min_body, max_body)
    let mut markers: BTreeMap<u16, u64> = BTreeMap::new();
    let mut pos = inner_start;
    let mut count = 0u64;
    while count < max_records {
        f.seek(SeekFrom::Start(pos)).unwrap();
        let mut hdr = [0u8; GHO_RECORD_HEADER_LEN];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let type_code = u16::from_le_bytes([hdr[0], hdr[1]]);
        let marker = u16::from_le_bytes([hdr[2], hdr[3]]);
        let magic = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
        let body_len = u16::from_le_bytes([hdr[8], hdr[9]]) as u32;
        if magic != GHO_RECORD_MAGIC {
            println!("STOP at {pos:#x}: bad magic {magic:#x} after {count} records");
            break;
        }
        let e = hist.entry(type_code).or_insert((0, 0, u32::MAX, 0));
        e.0 += 1;
        e.1 += body_len as u64;
        e.2 = e.2.min(body_len);
        e.3 = e.3.max(body_len);
        *markers.entry(marker).or_default() += 1;
        pos += GHO_RECORD_HEADER_LEN as u64 + body_len as u64;
        count += 1;
    }
    println!("\n{} records parsed up to {:#x}", count, pos);
    println!("Histogram by type_code:");
    for (t, (n, total, min, max)) in &hist {
        println!(
            "  type {t:#06x}: {n:>8} recs, body bytes {total:>14}, min {min:>6}, max {max:>6}"
        );
    }
    println!("Markers:");
    for (m, n) in &markers {
        println!("  marker {m:#06x}: {n} records");
    }
}
