//! Inject synthetic, *safe* transactions into a journaled HFS+ image so there
//! is something to look at / replay. Each transaction records the *current*
//! bytes of a metadata block and writes them to the journal only (not applied),
//! leaving the journal dirty. Replaying them rewrites identical bytes, so this
//! never corrupts the volume.
//!
//! Usage: cargo run --example craft_dirty_journal -- <image> [partition_offset] [n_txns]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::hfsplus_journal::{JournalHeader, JournalInfoBlock, TransactionBuilder};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: craft_dirty_journal <image> [offset] [n]");
    let offset: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(0);
    let n: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(2);

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open image read+write");

    // Read the volume header to find block size + journalInfoBlock.
    let mut vh = [0u8; 512];
    f.seek(SeekFrom::Start(offset + 1024)).unwrap();
    f.read_exact(&mut vh).unwrap();
    assert_eq!(&vh[0..2], &[0x48, 0x2b], "not an HFS+ volume header");
    let block_size = BigEndian::read_u32(&vh[40..44]) as u64;
    let jib_block = BigEndian::read_u32(&vh[12..16]) as u64;

    // Parse the JIB and journal header.
    let mut jib_buf = [0u8; 512];
    f.seek(SeekFrom::Start(offset + jib_block * block_size))
        .unwrap();
    f.read_exact(&mut jib_buf).unwrap();
    let jib = JournalInfoBlock::parse(&jib_buf).expect("parse JIB");

    let mut jh_buf = [0u8; 512];
    f.seek(SeekFrom::Start(offset + jib.offset)).unwrap();
    f.read_exact(&mut jh_buf).unwrap();
    let mut jh = JournalHeader::parse(&jh_buf).expect("parse journal header");
    println!(
        "journal: jhdr_size={} blhdr_size={} size={} start={} end={} seq={}",
        jh.jhdr_size, jh.blhdr_size, jh.size, jh.start, jh.end, jh.sequence_num
    );

    // Record the current bytes of a few jhdr_size-aligned blocks near the start
    // of the catalog B-tree region (any block works; this is reversible).
    let bs = jh.jhdr_size as u64;
    let base = BigEndian::read_u32(&vh[272 + 16..272 + 20]) as u64 * block_size; // catalog start block
    let base = (base / bs) * bs; // align to journal block size

    for i in 0..n {
        let target = base + i * bs;
        let mut cur = vec![0u8; bs as usize];
        f.seek(SeekFrom::Start(offset + target)).unwrap();
        f.read_exact(&mut cur).unwrap();

        let mut b = TransactionBuilder::new(jh.jhdr_size);
        b.record_block(target, &cur);
        b.write_to_journal(&mut f, &jib, &mut jh, offset)
            .expect("write transaction");
        println!(
            "  wrote txn for sector {} (seq now {})",
            target / 512,
            jh.sequence_num
        );
    }

    println!(
        "done — journal is now DIRTY (start={} end={}). \
         Run dump_journal to inspect, or open it in the GUI.",
        jh.start, jh.end
    );
}
