//! Multi-zone HD-format ADFS FSM walker. Built to validate the
//! Linux-derived address formula against a known-populated HD disc
//! (`CROS42.hdf`).
//!
//! For each zone:
//!   - Skip 4-byte zone header (32 bits) + (zone 0 only) 60-byte DR (480 bits).
//!   - Walk fragments via 15-bit-ID + variable-length-run + 1-bit-terminator.
//!   - Match against the target frag ID derived from `dr.root` split with idlen.
//!
//! Linux ADFS formulas verified against CROS42 + arc-04:
//!   zone_size_bits = (8 << log2_secsize) - zone_spare
//!   s_ids_per_zone = zone_size_bits / (idlen + 1)
//!   zone_for_frag  = frag_id / s_ids_per_zone
//!   zone N start in disc bytes = (zone_offset_byte) — typically zone-0-header
//!                                + N*zone_size_bytes.
//!
//! When the target frag is found at zone bit `start_bit`:
//!   scan_map_result = start_bit - dm_startbit + dm_startblk
//!   sector_addr = signed_asl(scan_map_result, log2_map_bits - log2_secsize)

use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

const ADFS_DR_SIZE_BITS: usize = 60 * 8;
const ZONE_HEADER_BITS: usize = 32;

/// Find the on-disc location of the FSM's zone 0 by scanning for a
/// secondary DR copy. The first 6 bytes of the DR (log2_secsize, spt,
/// heads, density, id_len, log2_map_bits) form a distinctive
/// fingerprint we can grep for.
fn find_fsm_base(f: &mut File, _sector_size: u64, dr_buf: [u8; 64]) -> u64 {
    // Use the first 6 DR bytes as the search needle.
    let needle: [u8; 6] = dr_buf[0..6].try_into().unwrap();
    let file_len = f.metadata().unwrap().len();
    let chunk_sz = 4 * 1024 * 1024usize;
    let mut pos = file_len / 4; // start search ~25% in (FSM is usually mid-disc)
    let mut found: Option<u64> = None;
    while pos < file_len {
        f.seek(SeekFrom::Start(pos)).unwrap();
        let mut buf = vec![0u8; chunk_sz + needle.len()];
        let n = f.read(&mut buf).unwrap_or(0);
        buf.truncate(n);
        let mut start = 0;
        while let Some(idx) = buf[start..].windows(needle.len()).position(|w| w == needle) {
            let hit = pos + (start + idx) as u64;
            // The DR proper begins 4 bytes into a zone (after the
            // zone header). So zone 0 of the FSM = hit - 4.
            if hit >= 4 {
                found = Some(hit - 4);
                break;
            }
            start += idx + 1;
        }
        if found.is_some() {
            break;
        }
        pos += chunk_sz as u64;
    }
    found.unwrap_or(0x400)
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: adfs_hd_zone_scout <disc.hdf>");
        std::process::exit(2);
    }
    let path = &args[0];
    let target_frag_arg: Option<u32> = args.get(1).and_then(|s| {
        s.strip_prefix("0x")
            .map(|h| u32::from_str_radix(h, 16).unwrap())
            .or_else(|| s.parse::<u32>().ok())
    });

    let mut f = File::open(path).expect("open disc");
    let len = f.metadata().unwrap().len();
    println!("== {path} ({len} bytes) ==");

    // Locate DR — same scan as the main reader.
    let candidates: &[u64] = &[0xFC0, 0xDC0, 0x404, 0x1FC0, 0x3FC0];
    let mut dr_offset = None;
    let mut dr_buf = [0u8; 64];
    for &cand in candidates {
        if f.seek(SeekFrom::Start(cand)).is_err() {
            continue;
        }
        let mut buf = [0u8; 64];
        if f.read_exact(&mut buf).is_err() {
            continue;
        }
        if (8..=11).contains(&buf[0]) && buf[9] != 0 {
            dr_offset = Some(cand);
            dr_buf = buf;
            break;
        }
    }
    let dr_off = dr_offset.expect("no DR found");
    println!("DR at byte 0x{dr_off:X}");

    let log2_ss = dr_buf[0];
    let sector_size = 1u32 << log2_ss;
    let id_len = dr_buf[4];
    let log2_map_bits = dr_buf[5];
    let zones = dr_buf[9] as u32;
    let zone_spare = LittleEndian::read_u16(&dr_buf[0x0A..0x0C]) as u32;
    let dr_root = LittleEndian::read_u32(&dr_buf[0x0C..0x10]);
    let disc_size = LittleEndian::read_u32(&dr_buf[0x10..0x14]);

    // Linux ADFS derived params.
    let zone_size_bits = ((8u64) << log2_ss) - zone_spare as u64; // bits / zone
    let zone_size_bytes = zone_size_bits / 8;
    let ids_per_zone = zone_size_bits / (id_len as u64 + 1);
    let map2blk: i32 = log2_map_bits as i32 - log2_ss as i32;

    println!(
        "DR: log2_ss={log2_ss} ({sector_size} B), id_len={id_len}, log2_map_bits={log2_map_bits}, \
         zones={zones}, zone_spare={zone_spare}, dr.root=0x{dr_root:X} ({dr_root}), \
         disc_size=0x{disc_size:X}",
    );
    println!(
        "Derived: zone_size_bits={zone_size_bits} ({zone_size_bytes} B), \
         ids_per_zone={ids_per_zone}, map2blk={map2blk}",
    );

    // Target fragment: dr.root split with idlen.
    let id_mask = (1u32 << id_len) - 1;
    let target_frag = target_frag_arg.unwrap_or(dr_root & id_mask);
    let root_offset = dr_root >> id_len;
    println!(
        "Target frag (from dr.root idlen-split): {} (0x{:X}), offset = {} sectors",
        target_frag, target_frag, root_offset,
    );

    // Where are zones laid out in disc bytes?
    //
    // For HD-format ADFS, the FSM is at a SECONDARY location (NOT
    // byte 0x400). The primary DR copy lives in the boot block at
    // byte 0xDC0; the FSM with its OWN secondary DR copy sits later
    // in the disc — at sector ~half-disc for the discs we've scouted.
    // Find it empirically by searching for additional DR copies.
    //
    // Each zone occupies exactly one sector (8 << log2_secsize bits =
    // sector_size bytes); the usable bits-per-zone is sector_size*8 -
    // zone_spare (the "spare" bits at the zone's tail are unused).
    let fsm_base = find_fsm_base(&mut f, sector_size as u64, dr_buf);
    println!("FSM base (zone-0-of-FSM) found at byte 0x{fsm_base:X}");

    let zone0_disc_byte = fsm_base;
    let zone_disc_byte = |zone: u32| -> u64 {
        // Zones are laid out adjacent at sector_size bytes apart.
        zone0_disc_byte + (zone as u64) * sector_size as u64
    };

    // Scan a single zone for the target frag, return (start_bit_in_zone,
    // end_bit_in_zone, alloc_bits_count, found_frag_id).
    let scan_zone = |f: &mut File, zone: u32| -> Vec<(u32, usize, usize)> {
        let zone_byte = zone_disc_byte(zone);
        if zone_byte >= len {
            return vec![];
        }
        let mut buf = vec![0u8; zone_size_bytes as usize];
        if f.seek(SeekFrom::Start(zone_byte)).is_err() {
            return vec![];
        }
        let n = f.read(&mut buf).unwrap_or(0);
        buf.truncate(n);

        let dm_startbit = if zone == 0 {
            ZONE_HEADER_BITS + ADFS_DR_SIZE_BITS
        } else {
            ZONE_HEADER_BITS
        };
        let dm_endbit = ZONE_HEADER_BITS + zone_size_bits as usize;
        let usable_end = dm_endbit.min(buf.len() * 8);

        let read_bits = |bits: &[u8], pos: usize, n: usize| -> u32 {
            let mut v = 0u32;
            for k in 0..n {
                let off = pos + k;
                if off / 8 >= bits.len() {
                    break;
                }
                let b = bits[off / 8];
                let bit = (b >> (off % 8)) & 1;
                v |= (bit as u32) << k;
            }
            v
        };

        let mut frags = Vec::new();
        let mut pos = dm_startbit;
        let mut count = 0;
        while pos + (id_len as usize) < usable_end && count < 5000 {
            let id = read_bits(&buf, pos, id_len as usize);
            let mut end = pos + id_len as usize;
            while end < usable_end {
                let byte = buf[end / 8];
                if (byte >> (end % 8)) & 1 == 1 {
                    break;
                }
                end += 1;
            }
            if end >= usable_end {
                break;
            }
            if id == 0 && end == pos + id_len as usize {
                // Empty hole — skip.
                pos = end + 1;
                count += 1;
                continue;
            }
            frags.push((id, pos, end));
            pos = end + 1;
            count += 1;
        }
        frags
    };

    println!();

    // Locate target frag's zone via Linux formula.
    let target_zone = (target_frag as u64)
        .checked_div(ids_per_zone)
        .map(|q| q as u32)
        .unwrap_or(0);
    println!("Linux ids_per_zone says: target_frag {target_frag} lives in zone {target_zone}");

    // Sweep all zones for the target frag (in case the formula is off).
    println!(
        "\nScanning all zones for frag {target_frag} (0x{:X}):",
        target_frag
    );
    for zone in 0..zones {
        let frags = scan_zone(&mut f, zone);
        let mut zone_target_hits = Vec::new();
        for (id, start, end) in &frags {
            if *id == target_frag {
                zone_target_hits.push((*start, *end));
            }
        }
        if !zone_target_hits.is_empty() {
            println!(
                "  zone {zone}: {} fragments, {} match for frag {target_frag}",
                frags.len(),
                zone_target_hits.len(),
            );
            for (start, end) in &zone_target_hits {
                let dm_startbit = if zone == 0 {
                    ZONE_HEADER_BITS + ADFS_DR_SIZE_BITS
                } else {
                    ZONE_HEADER_BITS
                };
                // dm_startblk per Linux:
                //   zone 0: 0
                //   zone N: N * zone_size_bits - ADFS_DR_SIZE_BITS
                let dm_startblk = if zone == 0 {
                    0i64
                } else {
                    (zone as i64) * (zone_size_bits as i64) - (ADFS_DR_SIZE_BITS as i64)
                };
                let result_after_adjust = (*start as i64) - (dm_startbit as i64) + dm_startblk;
                // signed_asl(result, map2blk):
                let sector_addr = if map2blk >= 0 {
                    result_after_adjust << map2blk
                } else {
                    result_after_adjust >> (-map2blk)
                };
                let disc_byte = sector_addr * (sector_size as i64);
                println!(
                    "    frag at zone-bit {start}..{end} → result {result_after_adjust} \
                     → sector {sector_addr} → disc byte 0x{:X}",
                    disc_byte,
                );

                // Search the area around the predicted disc byte for Nick magic.
                if disc_byte >= 0 && (disc_byte as u64) < len {
                    let probe_start = (disc_byte as u64).saturating_sub(64);
                    if f.seek(SeekFrom::Start(probe_start)).is_ok() {
                        let mut probe = [0u8; 256];
                        if f.read_exact(&mut probe).is_ok() {
                            for (i, w) in probe.windows(4).enumerate() {
                                if w == b"Nick" || w == b"Hugo" {
                                    let byte_addr = probe_start + i as u64;
                                    println!(
                                        "      ✓ {:?} magic at disc byte 0x{:X} (offset {:+} from predicted)",
                                        std::str::from_utf8(w).unwrap(),
                                        byte_addr,
                                        byte_addr as i64 - disc_byte,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
