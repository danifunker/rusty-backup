//! End-to-end HFS+ journal tests against *real macOS-created* journal images.
//!
//! The fixtures are flat journaled HFS+ "superfloppies" made on macOS with
//!   hdiutil create -size 4m -fs "Journaled HFS+" -volname JTest -layout NONE
//! then mounted and populated with:
//!   hello.txt          — "Hello, hfsplus-journaled!"
//!   subdir/            — directory
//!   subdir/nested.txt  — "nested file"
//! (plus macOS's own hidden .fseventsd), and checked in zstd-compressed. The
//! `_clean` image is exactly as macOS left it (empty journal). The `_dirty`
//! image additionally has three safe, self-referential transactions injected
//! via the `craft_dirty_journal` example (each records a metadata block's
//! current bytes, so replay rewrites identical data).
//!
//! These guard the parser/replayer against drift relative to genuine Apple
//! on-disk structures (byte order, JIB layout, journal-header checksum) — which
//! the synthetic unit tests in `src/fs/hfsplus_journal.rs` can't by themselves.
//!
//! Run with: cargo test --test hfsplus_journal_e2e

use byteorder::{BigEndian, ByteOrder};
use std::io::{Cursor, Read};

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::hfsplus::HfsPlusFilesystem;
use rusty_backup::fs::hfsplus_journal::{
    classify_target, replay_journal, JournalEndian, JournalHeader, JournalInfoBlock,
};

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .unwrap_or_else(|e| panic!("zstd decoder {path}: {e}"));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .unwrap_or_else(|e| panic!("decompress {path}: {e}"));
    out
}

/// Read the volume header and locate + parse the JIB and journal header the
/// same way the on-disk code does, for the replay test (which needs a writable
/// cursor and the JIB/header directly).
fn locate(img: &[u8]) -> (JournalInfoBlock, JournalHeader) {
    let vh = &img[1024..1536];
    assert_eq!(&vh[0..2], &[0x48, 0x2b], "not an HFS+ volume header");
    let block_size = BigEndian::read_u32(&vh[40..44]) as u64;
    let jib_block = BigEndian::read_u32(&vh[12..16]) as u64;
    let jib_off = (jib_block * block_size) as usize;
    let jib = JournalInfoBlock::parse(&img[jib_off..jib_off + 512]).expect("parse JIB");
    let jh_off = jib.offset as usize;
    let jh = JournalHeader::parse(&img[jh_off..jh_off + 512]).expect("parse journal header");
    (jib, jh)
}

#[test]
fn real_clean_journal_volume_has_files() {
    let img = load_fixture("test_hfsplus_journaled_clean.img.zst");
    let mut fs = HfsPlusFilesystem::open(Cursor::new(img), 0).expect("open HFS+");

    let root = fs.root().expect("root entry");
    let names: Vec<String> = fs
        .list_directory(&root)
        .expect("list root")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(
        names.iter().any(|n| n == "hello.txt"),
        "root names: {names:?}"
    );
    assert!(names.iter().any(|n| n == "subdir"), "root names: {names:?}");

    // Read hello.txt and confirm its contents survive the round-trip.
    let hello = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "hello.txt")
        .expect("hello.txt entry");
    let buf = fs.read_file(&hello, 1024).expect("read hello.txt");
    assert_eq!(buf, b"Hello, hfsplus-journaled!");

    // And the nested file under subdir/.
    let subdir = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "subdir")
        .expect("subdir entry");
    let nested: Vec<String> = fs
        .list_directory(&subdir)
        .expect("list subdir")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(
        nested.iter().any(|n| n == "nested.txt"),
        "subdir: {nested:?}"
    );
}

#[test]
fn real_clean_journal_parses_and_is_empty() {
    let img = load_fixture("test_hfsplus_journaled_clean.img.zst");
    let mut fs = HfsPlusFilesystem::open(Cursor::new(img), 0).expect("open HFS+");
    assert!(fs.is_journaled(), "fixture must be journaled");

    let detail = fs
        .journal_detail()
        .expect("read journal")
        .expect("journaled volume");

    // Genuine macOS journals from x86/arm are little-endian; the header
    // checksum is verified inside JournalHeader::parse, so reaching here at all
    // proves our 44-byte checksum matches Apple's.
    assert_eq!(detail.header.endian, JournalEndian::Little);
    assert!(
        detail.header.is_empty(),
        "freshly unmounted journal is clean"
    );
    assert!(detail.transactions.is_empty());
    assert!(detail.checksum_mismatch.is_none());
    assert!(detail.sequence_jumps.is_empty());
}

#[test]
fn real_dirty_journal_lists_injected_transactions() {
    let img = load_fixture("test_hfsplus_journaled_dirty.img.zst");
    let mut fs = HfsPlusFilesystem::open(Cursor::new(img), 0).expect("open HFS+");

    let detail = fs
        .journal_detail()
        .expect("read journal")
        .expect("journaled volume");

    assert!(!detail.header.is_empty(), "fixture journal must be dirty");
    assert_eq!(detail.transactions.len(), 3, "three injected transactions");
    assert!(detail.checksum_mismatch.is_none());
    assert!(
        detail.sequence_jumps.is_empty(),
        "sequence numbers are consecutive"
    );

    // Sequence numbers are strictly +1 and continue macOS's own counter.
    let seqs: Vec<u32> = detail.transactions.iter().map(|t| t.sequence_num).collect();
    assert_eq!(seqs[1], seqs[0] + 1);
    assert_eq!(seqs[2], seqs[1] + 1);

    // Each injected block targets the catalog B-tree region.
    for t in &detail.transactions {
        assert_eq!(t.blocks.len(), 1);
        let b = &t.blocks[0];
        assert_eq!(b.bsize, 512);
        assert_eq!(classify_target(&detail.regions, b.target), "catalog btree");
    }
}

#[test]
fn real_dirty_journal_replays_to_clean_without_changing_data() {
    let dirty = load_fixture("test_hfsplus_journaled_dirty.img.zst");
    let clean = load_fixture("test_hfsplus_journaled_clean.img.zst");

    // Capture the target sectors before replay so we can confirm replay wrote
    // exactly the (self-referential) bytes the journal carried.
    let (jib, mut jh) = locate(&dirty);
    let mut targets = Vec::new();
    {
        let mut fs = HfsPlusFilesystem::open(Cursor::new(dirty.clone()), 0).unwrap();
        for t in fs.journal_detail().unwrap().unwrap().transactions {
            targets.push(t.blocks[0].target);
        }
    }

    // Replay in place on a writable cursor.
    let mut disk = Cursor::new(dirty);
    let report = replay_journal(&mut disk, &jib, &mut jh, 0).expect("replay");
    assert_eq!(report.transactions_applied, 3);
    assert_eq!(report.blocks_written, 3);
    assert!(!report.partial);
    assert!(jh.is_empty(), "journal is clean after replay");

    // The injected transactions were self-referential, so the replayed image's
    // data blocks match the pristine clean image byte-for-byte.
    let replayed = disk.into_inner();
    for target in targets {
        let t = target as usize;
        assert_eq!(
            &replayed[t..t + 512],
            &clean[t..t + 512],
            "replayed sector {} differs from pristine volume",
            target / 512
        );
    }
}

/// The clean fixture must reflect what macOS wrote: parsing the JIB/header by
/// hand (no checksum tolerance) succeeds, confirming the fixture wasn't
/// accidentally regenerated with a broken layout.
#[test]
fn real_clean_journal_header_fields_are_sane() {
    let img = load_fixture("test_hfsplus_journaled_clean.img.zst");
    let (jib, jh) = locate(&img);
    assert!(jib.is_in_fs());
    assert!(!jib.is_external());
    assert_eq!(jh.jhdr_size, 512);
    assert!(jh.blhdr_size >= 512);
    assert!(jh.size >= jh.jhdr_size as u64);
    // start == end -> clean.
    assert_eq!(jh.start, jh.end);
    // Sanity: the JIB-declared journal size matches the header's.
    assert_eq!(jib.size, jh.size);
}
