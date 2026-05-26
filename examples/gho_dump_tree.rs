//! Structured dump of a file-aware GHO. Each record is shown with:
//!  - index, type, marker
//!  - for dir entries: decoded name + attr + cluster + size
//!  - for file content: size + offset within the current "file in progress"
//!  - for checksum: the u32 value
//!
//! Used to reverse-engineer the depth-first traversal algorithm
//! (especially: where do zero-pad sentinels imply popping vs. just
//! padding within a directory cluster?).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use rusty_backup::rbformats::gho::{
    find_inner_stream_start, is_boot_sector_record, is_checksum_record, is_data_block_record,
    is_dir_entry_record, parse_checksum_record_body, parse_fat_dir_entry_body, GhoContainerHeader,
    GHO_HEADER_PREFIX_LEN, GHO_PASSWORD_VERIFIER_LEN, GHO_RECORD_HEADER_LEN, GHO_RECORD_MAGIC,
};

fn main() {
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: gho_dump_tree <path> [max]"),
    );
    let max_records: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);

    let mut f = File::open(&path).unwrap();
    let header = GhoContainerHeader::parse(&mut f).unwrap();
    println!(
        "# file={} compression={:?} image_type={:?}",
        path.display(),
        header.compression,
        header.image_type
    );
    let header_end = (GHO_HEADER_PREFIX_LEN
        + if header.password_protected {
            GHO_PASSWORD_VERIFIER_LEN
        } else {
            0
        }) as u64;
    let inner_start = find_inner_stream_start(&mut f, header_end).unwrap();
    println!("# inner stream @ {:#x}", inner_start);
    println!();
    println!(
        "{:>4} {:>6} {:>6} {:>6}  {}",
        "#", "type", "marker", "body", "detail"
    );

    let mut pos = inner_start;
    let mut lfn_buf: Vec<String> = Vec::new();
    for idx in 0..max_records {
        f.seek(SeekFrom::Start(pos)).unwrap();
        let mut hdr = [0u8; GHO_RECORD_HEADER_LEN];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let type_code = u16::from_le_bytes([hdr[0], hdr[1]]);
        let marker = u16::from_le_bytes([hdr[2], hdr[3]]);
        let magic = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
        let body_len = u16::from_le_bytes([hdr[8], hdr[9]]) as usize;
        if magic != GHO_RECORD_MAGIC {
            println!("STOP at {pos:#x} (bad magic)");
            break;
        }
        let body_off = pos + GHO_RECORD_HEADER_LEN as u64;

        let detail = if is_boot_sector_record(type_code) {
            "boot sector (512 bytes)".to_string()
        } else if is_dir_entry_record(type_code) && body_len >= 36 {
            let mut body = vec![0u8; body_len];
            f.seek(SeekFrom::Start(body_off)).unwrap();
            f.read_exact(&mut body).unwrap();
            let e = parse_fat_dir_entry_body(&body).unwrap();
            decode_entry(&e, &mut lfn_buf)
        } else if is_data_block_record(type_code) {
            format!("data ({} bytes)", body_len)
        } else if is_checksum_record(type_code) && body_len >= 8 {
            let mut body = vec![0u8; body_len];
            f.seek(SeekFrom::Start(body_off)).unwrap();
            f.read_exact(&mut body).unwrap();
            let v = parse_checksum_record_body(&body).unwrap_or(0);
            format!("checksum={:#010x}", v)
        } else {
            format!("(body {} bytes)", body_len)
        };

        println!(
            "{:>4} {:#06x} {:#06x} {:>6}  {}",
            idx, type_code, marker, body_len, detail
        );

        pos = body_off + body_len as u64;
    }
}

fn decode_entry(
    e: &rusty_backup::rbformats::gho::GhoDirEntryRecord,
    lfn_buf: &mut Vec<String>,
) -> String {
    if e.is_empty_slot() {
        lfn_buf.clear();
        return "[empty slot]".to_string();
    }
    if e.is_deleted() {
        lfn_buf.clear();
        return "[deleted]".to_string();
    }
    if e.is_lfn_slot() {
        // LFN: sequence at byte 0 (mask 0x40 = last slot), name fragments
        // at bytes 1..11 (5 chars), 14..26 (6 chars), 28..32 (2 chars).
        let seq = e.fat_entry[0];
        let mut chars: Vec<u16> = Vec::with_capacity(13);
        for &range in &[(1usize, 11usize), (14, 26), (28, 32)] {
            let (s, end) = range;
            for chunk in e.fat_entry[s..end].chunks(2) {
                if chunk.len() == 2 {
                    let c = u16::from_le_bytes([chunk[0], chunk[1]]);
                    if c == 0 || c == 0xFFFF {
                        break;
                    }
                    chars.push(c);
                }
            }
        }
        let frag: String = char::decode_utf16(chars).filter_map(|r| r.ok()).collect();
        if seq & 0x40 != 0 {
            // Last-slot bit: this is the FIRST fragment in stream order
            // (LFNs are written in reverse).
            lfn_buf.clear();
        }
        lfn_buf.insert(0, frag.clone());
        format!("LFN seq={:#04x} frag={:?}", seq, frag)
    } else {
        let name_raw = &e.fat_entry[..11];
        let name = String::from_utf8_lossy(&name_raw[..8]).trim().to_string();
        let ext = String::from_utf8_lossy(&name_raw[8..11]).trim().to_string();
        let short = if ext.is_empty() {
            name
        } else {
            format!("{}.{}", name, ext)
        };
        let lfn = if !lfn_buf.is_empty() {
            let lfn = lfn_buf.join("");
            lfn_buf.clear();
            format!(" lfn={:?}", lfn)
        } else {
            String::new()
        };
        let kind = if e.is_volume_label() {
            "VOL"
        } else if e.is_directory() {
            "DIR"
        } else {
            "FILE"
        };
        format!(
            "{} 8.3={:?} attr={:#04x} cluster={} size={}{}",
            kind,
            short,
            e.attr_byte(),
            e.first_cluster(),
            e.file_size(),
            lfn
        )
    }
}
