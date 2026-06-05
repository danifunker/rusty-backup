//! Verify the Linux kernel's ADFS map walk formula against CROS42.hdf.
//!
//! Key insight from `fs/adfs/adfs.h::__adfs_block_map`:
//!   frag_id = indaddr >> 8
//!   offset_in_sectors = ((indaddr & 0xFF) - 1) << log2sharesize
//!
//! Then `adfs_map_lookup`:
//!   if frag_id == ADFS_ROOT_FRAG (2):
//!       zone_start = nzones >> 1     (middle of disc — where root frag lives)
//!   else:
//!       zone_start = frag_id / ids_per_zone
//!   mapoff = offset_in_sectors >> map2blk        (sector → map-bit)
//!   secoff = offset_in_sectors - (mapoff << map2blk)
//!   result = scan_map(zone_start, frag_id, &mut mapoff) returning
//!            (start_in_zone - dm_startbit) + dm_startblk
//!   physical_sector = secoff + (result << map2blk)
//!
//! Where `map2blk = log2bpmb - log2secsize` and:
//!   zone_size_bits = (8 << log2secsize) - zone_spare
//!   ids_per_zone   = zone_size_bits / (idlen + 1)
//!   dm[0].startblk = 0
//!   dm[0].startbit = 32 + 480              (32 b zone-hdr + 480 b DR copy)
//!   dm[N].startblk = N * zone_size_bits - 480
//!   dm[N].startbit = 32
//!   dm[N].endbit   = 32 + zone_size_bits    (clamped on last zone to disc end)
//!
//! Physical FSM location on disc (sectors):
//!   map_addr_bits = (nzones>>1) * zone_size_bits - ((nzones>1) ? 480 : 0)
//!   map_addr_sec  = signed_asl(map_addr_bits, map2blk)

use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

const ADFS_DR_SIZE_BITS: u64 = 60 * 8;
const ADFS_ROOT_FRAG: u32 = 2;

fn signed_asl_i64(val: i64, shift: i32) -> i64 {
    if shift >= 0 {
        val << shift
    } else {
        val >> (-shift)
    }
}

fn read_dr(f: &mut File, off: u64) -> [u8; 64] {
    let mut buf = [0u8; 64];
    f.seek(SeekFrom::Start(off)).unwrap();
    f.read_exact(&mut buf).unwrap();
    buf
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: adfs_fsm_probe <disc.hdf>");
        std::process::exit(2);
    }
    let path = &args[0];
    let mut f = File::open(path).expect("open");
    let file_len = f.metadata().unwrap().len();
    println!("== {path} ({} bytes) ==", file_len);

    // Locate DR: boot block path tries byte 0xDC0 first (E/F floppy + HD).
    // For HD, that's the canonical location.
    let candidates: &[u64] = &[0xDC0, 0xFC0, 0x404, 0x1FC0, 0x3FC0];
    let mut dr_off = None;
    let mut dr = [0u8; 64];
    for &c in candidates {
        let probe = read_dr(&mut f, c);
        let log2_ss = probe[0];
        if (8..=10).contains(&log2_ss) && probe[9] != 0 {
            dr_off = Some(c);
            dr = probe;
            break;
        }
    }
    let dr_byte = dr_off.expect("no DR found");
    println!("DR at byte 0x{dr_byte:X}");

    let log2_secsize = dr[0x00];
    let secs_per_track = dr[0x01];
    let heads = dr[0x02];
    let density = dr[0x03];
    let idlen = dr[0x04];
    let log2bpmb = dr[0x05]; // log2 bytes per map bit
    let skew = dr[0x06];
    let nzones_low = dr[0x09];
    let zone_spare = LittleEndian::read_u16(&dr[0x0A..0x0C]) as u32;
    let dr_root = LittleEndian::read_u32(&dr[0x0C..0x10]);
    let disc_size_lo = LittleEndian::read_u32(&dr[0x10..0x14]);
    let disc_size_high = LittleEndian::read_u32(&dr[0x24..0x28]);
    // log2sharesize is the low nibble of byte 0x28; big_flag is bit 0 of byte 0x29.
    let log2sharesize = dr[0x28] & 0x0F;
    let big_flag = dr[0x29] & 0x01;
    let nzones_high = dr[0x2A];
    let format_version = LittleEndian::read_u32(&dr[0x2C..0x30]);
    let root_size = LittleEndian::read_u32(&dr[0x30..0x34]);

    let sector_size = 1u32 << log2_secsize;
    let nzones = (nzones_low as u32) | ((nzones_high as u32) << 8);
    let zone_size_bits = ((8u64) << log2_secsize) - zone_spare as u64;
    let ids_per_zone = zone_size_bits / (idlen as u64 + 1);
    let map2blk: i32 = log2bpmb as i32 - log2_secsize as i32;
    let disc_size = ((disc_size_high as u64) << 32) | (disc_size_lo as u64);

    println!("DR: log2_secsize={log2_secsize} ({sector_size} B)");
    println!("    spt={secs_per_track} heads={heads} density={density} skew={skew}");
    println!(
        "    idlen={idlen} log2bpmb={log2bpmb} (1 map bit = {} B)",
        1u64 << log2bpmb
    );
    println!("    nzones={nzones} (lo={nzones_low} hi={nzones_high}) zone_spare={zone_spare}");
    println!("    zone_size_bits={zone_size_bits} ids_per_zone={ids_per_zone} map2blk={map2blk:+}");
    println!("    dr.root=0x{dr_root:X} ({dr_root})");
    println!(
        "      frag_id = dr.root>>8 = 0x{:X} ({})",
        dr_root >> 8,
        dr_root >> 8
    );
    println!(
        "      indaddr_lo = dr.root & 0xFF = 0x{:X} ({})",
        dr_root & 0xFF,
        dr_root & 0xFF
    );
    println!(
        "    disc_size=0x{disc_size:X} ({} bytes, {} MB)",
        disc_size,
        disc_size / (1024 * 1024)
    );
    println!("    log2sharesize={log2sharesize} big_flag={big_flag}");
    println!("    format_version={format_version} root_size={root_size}");

    // Predict FSM physical sector address:
    //   map_addr_bits = (nzones>>1) * zone_size_bits - ((nzones>1) ? 480 : 0)
    //   map_addr_sec  = signed_asl(map_addr_bits, map2blk)
    let map_addr_bits =
        (nzones as u64 >> 1) * zone_size_bits - if nzones > 1 { ADFS_DR_SIZE_BITS } else { 0 };
    let map_addr_sec = signed_asl_i64(map_addr_bits as i64, map2blk);
    let map_addr_byte = (map_addr_sec as u64) * sector_size as u64;
    println!();
    println!("Predicted FSM location:");
    println!(
        "  map_addr_bits = (nzones>>1)*zone_size - 480 = ({}/2)*{} - 480 = {}",
        nzones, zone_size_bits, map_addr_bits
    );
    println!(
        "  map_addr_sec = signed_asl({}, {:+}) = {}",
        map_addr_bits, map2blk, map_addr_sec
    );
    println!("  map_addr_byte = 0x{:X}", map_addr_byte);

    // Verify by reading the FSM zone-0 and checking for DR fingerprint at byte 4.
    // First 6 bytes of the DR match (log2_ss, spt, heads, density, idlen, log2bpmb).
    let needle = &dr[0..6];
    let mut zone0_probe = vec![0u8; 64];
    f.seek(SeekFrom::Start(map_addr_byte + 4)).unwrap();
    let n = f.read(&mut zone0_probe).unwrap();
    zone0_probe.truncate(n);
    let dr_matches = zone0_probe.starts_with(needle);
    println!(
        "  Zone-0-DR fingerprint at FSM+4: {}",
        if dr_matches { "MATCH" } else { "MISMATCH" }
    );

    // Resolve dr.root through the formula and compare to expected root location.
    let frag_id = dr_root >> 8;
    let indaddr_lo = (dr_root & 0xFF) as i32;
    let block_offset_sec: i64 = if indaddr_lo != 0 {
        ((indaddr_lo - 1) as i64) << log2sharesize
    } else {
        0
    };
    let mapoff = signed_asl_i64(block_offset_sec, -map2blk);
    let secoff = block_offset_sec - signed_asl_i64(mapoff, map2blk);

    println!();
    println!("Resolving dr.root=0x{dr_root:X}:");
    println!("  frag_id={frag_id} indaddr_lo={indaddr_lo}");
    println!("  block_offset_sec = ({indaddr_lo}-1)<<{log2sharesize} = {block_offset_sec}");
    println!(
        "  mapoff (in map bits) = {block_offset_sec}>>{:+} = {mapoff}",
        -map2blk
    );
    println!(
        "  secoff = {block_offset_sec} - ({mapoff}<<{:+}) = {secoff}",
        map2blk
    );

    // For root frag (2), scan starts at zone nzones>>1.
    let zone_start = if frag_id == ADFS_ROOT_FRAG {
        nzones >> 1
    } else {
        (frag_id / ids_per_zone as u32) % nzones
    };
    println!(
        "  zone_start = {zone_start} (root_frag={})",
        frag_id == ADFS_ROOT_FRAG
    );

    // Walk fragments in zone_start looking for frag_id.
    // dm_startbit / dm_startblk per kernel adfs_map_layout:
    let dm_startbit = if zone_start == 0 {
        32 + ADFS_DR_SIZE_BITS as u32
    } else {
        32
    };
    let dm_startblk: i64 = if zone_start == 0 {
        0
    } else {
        (zone_start as i64) * (zone_size_bits as i64) - ADFS_DR_SIZE_BITS as i64
    };
    let dm_endbit = 32 + zone_size_bits as u32;
    println!("  zone {zone_start}: dm_startbit={dm_startbit} dm_startblk={dm_startblk} dm_endbit={dm_endbit}");

    // Read the zone bytes.
    let zone_byte = map_addr_byte + (zone_start as u64) * (sector_size as u64);
    let mut zone_buf = vec![0u8; sector_size as usize];
    f.seek(SeekFrom::Start(zone_byte)).unwrap();
    let n = f.read(&mut zone_buf).unwrap_or(0);
    zone_buf.truncate(n);
    println!("  zone {zone_start} at byte 0x{:X} ({n} B read)", zone_byte);

    let id_mask = (1u32 << idlen) - 1;
    let read_bits = |buf: &[u8], pos: u32, n: u32| -> u32 {
        let mut v = 0u32;
        for k in 0..n {
            let off = (pos + k) as usize;
            if off / 8 >= buf.len() {
                break;
            }
            let b = buf[off / 8];
            let bit = (b >> (off % 8)) & 1;
            v |= (bit as u32) << k;
        }
        v
    };

    // Iterate as kernel `lookup_zone`.
    let mut start = dm_startbit;
    let mut remaining_mapoff = mapoff;
    let mut found_at: Option<(u32, u32, u32)> = None; // (start_bit, fragend, length)
    let mut iter = 0;
    let idlen_u32 = idlen as u32;
    while start + idlen_u32 < dm_endbit && iter < 10000 {
        iter += 1;
        let frag = read_bits(&zone_buf, start, idlen_u32);
        // find_next_bit_le starts AT start+idlen, looking for a set bit.
        let search_from = start + idlen_u32;
        let mut fragend = search_from;
        while fragend < dm_endbit {
            let byte = zone_buf[(fragend / 8) as usize];
            if (byte >> (fragend % 8)) & 1 == 1 {
                break;
            }
            fragend += 1;
        }
        if fragend >= dm_endbit {
            break;
        }
        let length = fragend + 1 - start;
        let m = frag & id_mask;
        if m == frag_id {
            if remaining_mapoff < length as i64 {
                found_at = Some((start + remaining_mapoff as u32, fragend, length));
                break;
            }
            remaining_mapoff -= length as i64;
        }
        start = fragend + 1;
    }

    match found_at {
        Some((hit_bit, fragend, length)) => {
            println!(
                "  frag {frag_id} found: start_bit={hit_bit} fragend={fragend} length={length}"
            );
            let result_bits = (hit_bit as i64) - (dm_startbit as i64) + dm_startblk;
            let result_sec = signed_asl_i64(result_bits, map2blk);
            let final_sec = secoff + result_sec;
            let final_byte = final_sec * sector_size as i64;
            println!("  result_bits = {hit_bit}-{dm_startbit}+{dm_startblk} = {result_bits}");
            println!("  result_sec = signed_asl({result_bits}, {map2blk:+}) = {result_sec}");
            println!("  final_sec = {secoff}+{result_sec} = {final_sec}");
            println!("  final_byte = 0x{final_byte:X}");

            // Sniff that area for Hugo/Nick magic to confirm we landed on the root.
            if final_byte > 0 && (final_byte as u64) < file_len {
                let probe_start = (final_byte as u64).saturating_sub(8);
                f.seek(SeekFrom::Start(probe_start)).unwrap();
                let mut probe = [0u8; 64];
                let n = f.read(&mut probe).unwrap_or(0);
                for (i, w) in probe[..n.min(60)].windows(4).enumerate() {
                    if w == b"Hugo" || w == b"Nick" {
                        let where_at = probe_start + i as u64;
                        println!(
                            "  ✓ {:?} at byte 0x{:X} (offset {:+} from predicted)",
                            std::str::from_utf8(w).unwrap(),
                            where_at,
                            where_at as i64 - final_byte
                        );
                    }
                }
            }
        }
        None => {
            println!("  frag {frag_id} not found in zone {zone_start}");
        }
    }
}
