//! Dump the HFS+ journal of an image, for poking at journaled volumes.
//!
//! Usage: cargo run --example dump_journal -- <image> [partition_offset]
//!
//! `partition_offset` defaults to 0 (a flat HFS+ "superfloppy", e.g. an image
//! made with `hdiutil create -fs "Journaled HFS+" -layout NONE`).

use std::fs::File;

use rusty_backup::fs::hfsplus::HfsPlusFilesystem;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: dump_journal <image> [offset]");
    let offset: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(0);

    let file = File::open(&path).expect("open image");
    let mut fs = HfsPlusFilesystem::open(file, offset).expect("parse HFS+ volume header");

    println!("volume journaled bit: {}", fs.is_journaled());

    match fs.journal_detail().expect("read journal") {
        None => println!("(volume is not journaled)"),
        Some(d) => {
            let h = &d.header;
            println!(
                "journal: offset={} size={} jhdr_size={} blhdr_size={} endian={:?}",
                d.info.offset, d.info.size, h.jhdr_size, h.blhdr_size, h.endian
            );
            println!(
                "         start={} end={} sequence_num={} -> {}",
                h.start,
                h.end,
                h.sequence_num,
                if h.is_empty() {
                    "CLEAN (empty)"
                } else {
                    "DIRTY"
                }
            );
            if let Some(seq) = d.checksum_mismatch {
                println!("         checksum mismatch at sequence {seq}");
            }
            for (from, to) in &d.sequence_jumps {
                println!("         sequence jump {from} -> {to}");
            }
            println!("metadata regions (partition-relative bytes):");
            for r in &d.regions {
                println!("  {:<18} {}..{}", r.name, r.start, r.end);
            }
            println!("{} pending transaction(s):", d.transactions.len());
            for t in &d.transactions {
                let range = t.blocks.iter().fold(None::<(u64, u64)>, |acc, b| {
                    let hi = b.target + b.bsize as u64;
                    match acc {
                        None => Some((b.target, hi)),
                        Some((lo, phi)) => Some((lo.min(b.target), phi.max(hi))),
                    }
                });
                println!(
                    "  #{}  {} block(s)  {} bytes  sectors {}",
                    t.sequence_num,
                    t.blocks.len(),
                    t.total_bytes,
                    match range {
                        Some((lo, hi)) => format!("{}..{}", lo / 512, hi / 512),
                        None => "-".into(),
                    }
                );
                for b in &t.blocks {
                    let class =
                        rusty_backup::fs::hfsplus_journal::classify_target(&d.regions, b.target);
                    println!(
                        "      sector {:<8} {} bytes  [{}]",
                        b.target / 512,
                        b.bsize,
                        class
                    );
                }
            }
        }
    }
}
