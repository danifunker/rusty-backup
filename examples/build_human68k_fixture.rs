//! Build a Human68k-shaped FAT12 fixture: starts from an mkfs.fat-formatted
//! flat image and uses our own EditableFilesystem to write the seed files,
//! so directory bytes 12-21 are zero (no spurious "extended name" bytes
//! from FAT-control fields that mtools' mcopy would write).
//!
//! Usage: build_human68k_fixture <flat_in.img> <flat_out.img>
//!
//! Used by `scripts/generate-d88-fixture.sh`.

use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::human68k::Human68kFilesystem;
use std::fs::OpenOptions;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: build_human68k_fixture <flat_in.img> <flat_out.img>");
        std::process::exit(2);
    }
    let in_path = &args[1];
    let out_path = &args[2];
    std::fs::copy(in_path, out_path).expect("copy in→out");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path)
        .expect("open out for r/w");
    let mut fs = Human68kFilesystem::open(file, 0).expect("open Human68k");
    let root = fs.root().expect("root");
    let hello_payload: &[u8] = b"Hello from D88 X68000 fixture.";
    let hello_len = hello_payload.len() as u64;
    let mut hello_reader: &[u8] = hello_payload;
    fs.create_file(
        &root,
        "HELLO.TXT",
        &mut hello_reader,
        hello_len,
        &CreateFileOptions::default(),
    )
    .expect("create HELLO.TXT");
    let note_payload: &[u8] = b"Second file, different content.";
    let note_len = note_payload.len() as u64;
    let mut note_reader: &[u8] = note_payload;
    fs.create_file(
        &root,
        "NOTE.TXT",
        &mut note_reader,
        note_len,
        &CreateFileOptions::default(),
    )
    .expect("create NOTE.TXT");
    fs.sync_metadata().expect("sync");
    eprintln!("built {} with HELLO.TXT + NOTE.TXT via Human68k", out_path);
}
