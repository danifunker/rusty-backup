// Dump every NTFS cluster-run header's (seq, cc) tuple in stream order from
// an uncompressed file-aware GHO. If the seq numbers strictly increase from
// 0/1, they are the missing MFT-data-run index we've been throwing away.
//
// Usage:
//   cargo run --release --example gho_ntfs_seq_probe -- PATH.GHO [MAX]

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn rd_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes([
        b[o],
        b[o + 1],
        b[o + 2],
        b[o + 3],
        b[o + 4],
        b[o + 5],
        b[o + 6],
        b[o + 7],
    ])
}

// Entry A (16 bytes) — only present at the start of the MFT run header.
const ENTRY_A: [u8; 16] = [
    0x0E, 0x20, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,
];

// Entry B fixed bytes (0-6 and 15). Bytes 7-14 are the u64 sequence number.
fn match_entry_b(w: &[u8]) -> bool {
    w.len() >= 16
        && w[0] == 0x0E
        && w[1] == 0x02
        && w[2] == 0x00
        && w[3] == 0x00
        && w[4] == 0x00
        && w[5] == 0x00
        && w[6] == 0x00
        && w[15] == 0x0F
}

fn main() -> std::io::Result<()> {
    let path = std::env::args().nth(1).expect("path");
    let max: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);

    let mut f = File::open(&path)?;
    let file_size = f.metadata()?.len();

    // Read entire file (smallNTFS is ~1.8 GB, may want to bound). Stream
    // through a chunked scan instead.
    const CHUNK: usize = 4 * 1024 * 1024;
    let mut buf = vec![0u8; CHUNK + 64];
    let mut pos: u64 = 0;
    let mut printed = 0usize;
    let mut tail = Vec::new();
    let mut first_mft_found_at: Option<u64> = None;

    let mut last_seq: i128 = -1;
    let mut nonmono = 0u64;
    let mut total_b = 0u64;

    while pos < file_size {
        f.seek(SeekFrom::Start(pos))?;
        let want = ((file_size - pos) as usize).min(CHUNK + 64);
        let n = f.read(&mut buf[..want])?;
        if n == 0 {
            break;
        }
        let mut scan = Vec::with_capacity(tail.len() + n);
        scan.extend_from_slice(&tail);
        scan.extend_from_slice(&buf[..n]);

        let mut i = 0;
        while i + 26 <= scan.len() {
            let win = &scan[i..i + 16];
            if first_mft_found_at.is_none() && win == ENTRY_A {
                let abs = pos + i as u64 - tail.len() as u64;
                first_mft_found_at = Some(abs);
                // 3-entry header: entry A (16) + entry B (16) + entry C (10) = 42 bytes
                // MFT cc is at offset 33..41 (entry C's u64 length).
                if i + 42 <= scan.len() {
                    let cc = rd_u64(&scan[i..], 33);
                    println!("MFT @ {:#x}: cc={cc} (3-entry header)", abs);
                }
                i += 1;
                continue;
            }
            if match_entry_b(win) {
                let abs = pos + i as u64 - tail.len() as u64;
                // Entry B (16) + Entry C (10) = 26 bytes
                if i + 26 <= scan.len() && scan[i + 16] == 0x0F && scan[i + 25] == 0x0E {
                    let seq = rd_u64(&scan[i..], 7);
                    let cc = rd_u64(&scan[i..], 17);
                    total_b += 1;
                    let s = seq as i128;
                    if s <= last_seq {
                        nonmono += 1;
                    }
                    last_seq = s;
                    if printed < max {
                        println!("run @ {:#x}: seq={seq} cc={cc}", abs);
                        printed += 1;
                    }
                    i += 26 + (cc as usize * 4096).min(file_size as usize); // skip past run data (assume 4 KiB cluster)
                    continue;
                }
            }
            i += 1;
        }

        // Carry last 26 bytes for boundary straddle.
        if scan.len() >= 26 {
            tail = scan[scan.len() - 26..].to_vec();
        } else {
            tail = scan;
        }
        pos += n as u64;
    }

    println!("\n--- summary ---");
    println!("MFT entry A found at: {:?}", first_mft_found_at);
    println!("entry B runs scanned: {total_b}");
    println!("non-monotonic seq transitions: {nonmono}");
    println!("max printed: {printed}/{max}");
    Ok(())
}
