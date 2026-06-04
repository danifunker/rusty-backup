//! Empirical scout for the ADFS `dr.root` encoding on E-format discs.
//!
//! Dumps the Disc Record, walks zone 0 of the FSM, and prints every
//! candidate decoding of `dr.root` we can think of so we can match
//! against the known location of the root dir (`Nick` magic).
//!
//! Run: `cargo run --example adfs_dr_root_scout <path>`

use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: adfs_dr_root_scout <disc.adf> [<disc2.adf> ...]");
        std::process::exit(2);
    }
    for path in args {
        println!("\n==== {path} ====");
        let mut f = File::open(&path).expect("open disc");
        let len = f.metadata().unwrap().len();
        println!("  file size: {len} bytes");

        // 1. Locate the Disc Record. Try the same scan candidates the
        //    main reader uses.
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
            // Quick syntactic check: log2_sector_size in 8..=11, zones != 0.
            let log2_ss = buf[0];
            let zones = buf[9];
            if (8..=11).contains(&log2_ss) && zones != 0 {
                dr_offset = Some(cand);
                dr_buf = buf;
                break;
            }
        }
        let dr_off = match dr_offset {
            Some(o) => o,
            None => {
                println!("  NO disc record found at scan candidates");
                continue;
            }
        };
        println!("  Disc Record found at byte 0x{dr_off:X}");

        let log2_ss = dr_buf[0];
        let sector_size = 1u32 << log2_ss;
        let sectors_per_track = dr_buf[1];
        let heads = dr_buf[2];
        let density = dr_buf[3];
        let id_len = dr_buf[4];
        let log2_map_bits = dr_buf[5];
        let skew = dr_buf[6];
        let _boot_option = dr_buf[7];
        let _low_sector = dr_buf[8];
        let zones = dr_buf[9];
        let zone_spare = LittleEndian::read_u16(&dr_buf[0x0A..0x0C]);
        let root = LittleEndian::read_u32(&dr_buf[0x0C..0x10]);
        let disc_size = LittleEndian::read_u32(&dr_buf[0x10..0x14]);
        let disc_id = LittleEndian::read_u16(&dr_buf[0x14..0x16]);

        println!(
            "  DR fields: log2_ss={log2_ss} ({sector_size} B), spt={sectors_per_track}, \
             heads={heads}, density={density}, id_len={id_len}, log2_map_bits={log2_map_bits}, \
             skew={skew}, zones={zones}, zone_spare={zone_spare}, disc_id=0x{disc_id:04X}",
        );
        println!(
            "  dr.root = 0x{root:X} = {root}, dr.disc_size_bytes = 0x{disc_size:X} = {disc_size}",
        );

        // 2. Candidate decodings of dr.root.
        //
        // Split dr.root into (frag_id, sector_offset) with the
        // configured id_len.
        let id_mask: u32 = (1u32 << id_len) - 1;
        let frag_id_low = root & id_mask;
        let sector_off_high = root >> id_len;
        println!(
            "  candidate A: id_len={id_len} split → frag={frag_id_low}, sector_off={sector_off_high}",
        );

        for il in [7u8, 8, 11, 13, 14, 15] {
            let m = (1u32 << il) - 1;
            let f = root & m;
            let s = root >> il;
            println!("  candidate B(id_len={il}): frag={f}, sector_off={s}",);
        }

        // 3. Where's the Nick magic? Scan early sectors and report any hit.
        //    Nick lives at byte 1 of the directory header.
        println!("  searching for 'Nick' magic in the first 64 KiB...");
        let mut buf = vec![0u8; (64 * 1024).min(len as usize)];
        f.seek(SeekFrom::Start(0)).unwrap();
        f.read_exact(&mut buf).unwrap();
        let mut found = 0;
        for (i, w) in buf.windows(4).enumerate() {
            if w == b"Nick" || w == b"Hugo" {
                println!(
                    "    {:?} at byte 0x{:X} (sector {})",
                    std::str::from_utf8(w).unwrap(),
                    i,
                    i as u32 / sector_size,
                );
                found += 1;
                if found >= 8 {
                    break;
                }
            }
        }

        // 4. Dump zone 0 raw bytes. For E-format, zone 0 starts at byte
        //    0x400 (sector 1). Each zone is `zone_spare`-bit-tagged
        //    fragments. We just print the first 256 bytes for eyeball.
        let zone_off = 0x400u64;
        if f.seek(SeekFrom::Start(zone_off)).is_ok() {
            let mut z = [0u8; 256];
            if f.read_exact(&mut z).is_ok() {
                println!("  zone 0 (byte 0x{zone_off:X}), first 64 bytes:");
                for chunk in z[..64].chunks(16) {
                    print!("    ");
                    for b in chunk {
                        print!("{:02X} ", b);
                    }
                    println!();
                }
            }
        }

        // 5. Print a small window AROUND byte 0x800 and 0x400 so we can
        //    see what's actually at the supposed root location.
        for &probe in &[0x400u64, 0x800, 0x1000, 0x2000] {
            if f.seek(SeekFrom::Start(probe)).is_ok() {
                let mut b = [0u8; 16];
                if f.read_exact(&mut b).is_ok() {
                    print!("  bytes @ 0x{probe:X}: ");
                    for x in b {
                        print!("{:02X} ", x);
                    }
                    println!();
                }
            }
        }

        // 6. Walk the FSM bitmap and enumerate fragments. For E-format
        //    we expect 1 zone, zone-0 bit-stream begins after the DR
        //    (zone byte 4 + 60 = 64), runs for the rest of zone 0.
        //    Each fragment is encoded as a run of 0 bits followed by
        //    its idlen-bit ID, terminated by a 1 bit. The bit-stream
        //    is little-endian within each byte (bit 0 = LSB).
        //
        //    Total bits per zone = (zone size in bytes) * 8 -
        //    zone_header_bits (4*8 = 32) - dr_bits (60*8 = 480) -
        //    zone_spare (from DR).
        //
        //    For E-format the zone is exactly 800 sectors / 1 zone =
        //    sector_size * 800 = 800 * 1024 = 0xC8000 bytes. But the
        //    zone actually starts at byte 0x400 (sector 1) and runs
        //    until... let me just dump and we'll see.

        // Read all of zone 0 — from byte 0x400 to end of disk for now.
        let zone_start = 0x400u64;
        let zone_size = if zones == 1 {
            len - zone_start
        } else {
            (disc_size as u64) / zones as u64
        };
        let mut zone_bytes = vec![0u8; zone_size as usize];
        f.seek(SeekFrom::Start(zone_start)).unwrap();
        f.read_exact(&mut zone_bytes).unwrap();

        // Skip 4-byte header + 60-byte DR (for zone 0) = 64 bytes,
        // then the FSM bitmap follows.
        let fsm_start_bit = 64 * 8;
        let total_bits_in_zone = zone_size as usize * 8;
        let usable_bits = total_bits_in_zone - zone_spare as usize;
        println!(
            "  zone 0 size = {zone_size} B, total bits = {total_bits_in_zone}, \
                  usable = {usable_bits} (zone_spare = {zone_spare})"
        );

        // Walk the bitstream. Each fragment ID has an idlen-bit value
        // followed by allocated map bits (0 = not in this run) until
        // the next "1" terminator bit (length of run varies).
        //
        // Linux kernel layout per `adfs/map.c`:
        //   read_idlen-bit ID; the BIT *position* where the ID starts
        //   acts as the fragment's allocated map_bit_address.
        //   The run continues with 0 bits until the next "1" terminator.
        //
        // i.e., the FSM is a series of (id, allocated-run, terminator) tuples.
        // The total map bits covered by each fragment is the length of
        // its allocated run.

        let read_bits = |bits: &[u8], pos: usize, n: usize| -> u32 {
            let mut v = 0u32;
            for k in 0..n {
                let b = bits[(pos + k) / 8];
                let bit = (b >> ((pos + k) % 8)) & 1;
                v |= (bit as u32) << k;
            }
            v
        };

        let mut pos = fsm_start_bit;
        let mut frag_count = 0;
        let mut frags: Vec<(u32, usize, usize)> = Vec::new(); // (id, start_bit, end_bit)
        while pos + id_len as usize <= usable_bits && frag_count < 50 {
            let id = read_bits(&zone_bytes, pos, id_len as usize);
            // Find the terminator bit (next 1 after the id field).
            let mut end = pos + id_len as usize;
            while end < usable_bits {
                let b = zone_bytes[end / 8];
                if (b >> (end % 8)) & 1 == 1 {
                    break;
                }
                end += 1;
            }
            // The terminator bit at `end` is included; advance past it.
            if id == 0 && end == pos + id_len as usize {
                // Empty / sentinel — stop walking.
                break;
            }
            frags.push((id, pos, end));
            pos = end + 1;
            frag_count += 1;
        }

        println!("  walked {} fragments from zone 0 FSM:", frags.len());
        for (id, start, end) in frags.iter().take(30) {
            let map_bits_covered = end - start + 1;
            let _ = start as *const usize;
            println!(
                "    frag id={:5} (0x{:03X}) at zone-bit {}..{}, map_bits={}, ≈ disc byte 0x{:X}",
                id,
                id,
                *start,
                *end,
                map_bits_covered,
                zone_start + ((*start as u64) << log2_map_bits) / 8,
            );
        }

        // 7. Decode dr.root with multiple hypotheses against the
        //    actual fragment list.
        println!("  --- dr.root decoding hypotheses ---");
        let frag_lookup = |target: u32| -> Option<&(u32, usize, usize)> {
            frags.iter().find(|(id, _, _)| *id == target)
        };

        // H1: id_len=15 (declared)
        let f15 = root & 0x7FFF;
        let o15 = root >> 15;
        let f15_disc = frag_lookup(f15).map(|(id, s, _)| {
            let map_bit = *s;
            let disc_byte = (map_bit as u64) << log2_map_bits;
            (id, map_bit, disc_byte)
        });
        println!(
            "  H1 (id_len=15): frag={f15}, offset_sectors={o15}, frag found = {:?}",
            f15_disc
        );

        // H2: id_len=8 (would split 0x203 → frag=3, offset=2)
        let f8 = root & 0xFF;
        let o8 = root >> 8;
        let f8_disc = frag_lookup(f8).map(|(id, s, _)| {
            let map_bit = *s;
            let disc_byte = (map_bit as u64) << log2_map_bits;
            (id, map_bit, disc_byte)
        });
        println!(
            "  H2 (id_len=8): frag={f8}, offset_sectors={o8}, frag found = {:?}",
            f8_disc
        );

        // H3: dr.root is itself the bit position of the fragment in zone 0.
        if (root as usize) < zone_bytes.len() * 8 {
            let id = read_bits(&zone_bytes, root as usize, id_len as usize);
            println!(
                "  H3 (dr.root = zone-bit position): id at bit {} = {} (frag this means)",
                root, id
            );
        }

        // H4: ADFS_ROOT_FRAG=2 hardcoded, dr.root is offset within frag.
        if let Some((id, s, e)) = frag_lookup(2) {
            let map_bits_in_frag = e - s + 1;
            let disc_byte_of_frag_start = (*s as u64) << log2_map_bits;
            let frag_end_byte =
                disc_byte_of_frag_start + ((map_bits_in_frag as u64) << log2_map_bits);
            println!(
                "  H4 (frag 2 is root by Filecore convention): frag 2 id={}, spans bits {}..{} ({} bits),",
                id, s, e, map_bits_in_frag,
            );
            println!(
                "       maps to disc bytes 0x{:X}..0x{:X}",
                disc_byte_of_frag_start, frag_end_byte,
            );
            println!(
                "       dr.root={} (0x{:X}): byte-in-frag interpretation = byte 0x{:X}",
                root,
                root,
                disc_byte_of_frag_start + root as u64,
            );
        }
    }
}
