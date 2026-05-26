//! Dump the first N records of a file-aware GHO with their body bytes,
//! so we can reverse-engineer the body layouts for 0x0004 / 0x0102 /
//! 0x0103 / 0x0104.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use rusty_backup::rbformats::gho::{
    find_inner_stream_start, GhoContainerHeader, GHO_HEADER_PREFIX_LEN, GHO_PASSWORD_VERIFIER_LEN,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| {
        std::env::var("HOME").unwrap() + "/new-fixtures/gho/ManualGhostBackups/7.5/PART/PART.GHO"
    }));
    let max_records: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(60);

    let mut f = File::open(&path).unwrap();
    let header = GhoContainerHeader::parse(&mut f).unwrap();
    println!(
        "file={} compression={:?} image_type={:?} password_protected={}",
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

    let mut pos = inner_start;
    for i in 0..max_records {
        f.seek(SeekFrom::Start(pos)).unwrap();
        let mut hdr = [0u8; 10];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let type_code = u16::from_le_bytes([hdr[0], hdr[1]]);
        let marker = u16::from_le_bytes([hdr[2], hdr[3]]);
        let magic = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
        let body_len = u16::from_le_bytes([hdr[8], hdr[9]]) as usize;
        if magic != 0x012F18D8 {
            println!("#{i} BAD magic {magic:#x} at {pos:#x}; stopping");
            break;
        }
        let body_off = pos + 10;
        let dump = body_len.min(128);
        let mut body = vec![0u8; dump];
        f.seek(SeekFrom::Start(body_off)).unwrap();
        f.read_exact(&mut body).unwrap();
        println!(
            "#{i:3} type={type_code:#06x} marker={marker:#06x} body_len={body_len:5} body_off={body_off:#010x}",
        );
        // Hex dump (first 128 bytes only).
        for chunk in body.chunks(32) {
            print!("    ");
            for b in chunk {
                print!("{b:02x} ");
            }
            print!("   ");
            for b in chunk {
                if (0x20..0x7f).contains(b) {
                    print!("{}", *b as char);
                } else {
                    print!(".");
                }
            }
            println!();
        }
        pos = body_off + body_len as u64;
    }
}
