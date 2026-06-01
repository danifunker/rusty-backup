//! Replay a dirty HFS+ journal in place (like macOS does on mount), then
//! report. Safe to run on an image crafted by `craft_dirty_journal`.
//!
//! Usage: cargo run --example replay_journal -- <image> [partition_offset]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::hfsplus_journal::{replay_journal, JournalHeader, JournalInfoBlock};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: replay_journal <image> [offset]");
    let offset: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(0);

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open image read+write");

    let mut vh = [0u8; 512];
    f.seek(SeekFrom::Start(offset + 1024)).unwrap();
    f.read_exact(&mut vh).unwrap();
    let block_size = BigEndian::read_u32(&vh[40..44]) as u64;
    let jib_block = BigEndian::read_u32(&vh[12..16]) as u64;

    let mut jib_buf = [0u8; 512];
    f.seek(SeekFrom::Start(offset + jib_block * block_size))
        .unwrap();
    f.read_exact(&mut jib_buf).unwrap();
    let jib = JournalInfoBlock::parse(&jib_buf).expect("parse JIB");

    let mut jh_buf = [0u8; 512];
    f.seek(SeekFrom::Start(offset + jib.offset)).unwrap();
    f.read_exact(&mut jh_buf).unwrap();
    let mut jh = JournalHeader::parse(&jh_buf).expect("parse journal header");

    let report = replay_journal(&mut f, &jib, &mut jh, offset).expect("replay");
    println!(
        "replayed {} transaction(s), {} block(s), {} bytes; partial={}; last_seq={}",
        report.transactions_applied,
        report.blocks_written,
        report.bytes_written,
        report.partial,
        report.last_sequence_num,
    );
    println!(
        "journal now: start={} end={} -> {}",
        jh.start,
        jh.end,
        if jh.is_empty() {
            "CLEAN"
        } else {
            "still dirty"
        }
    );
}
