//! Atari DOS cross-validation tool. Bridges the rusty-backup `atari_dos`
//! engine and an external reference (atrcopy / a clean-room reader).
//!
//! Usage:
//!   atari_xcheck dump  <in.atr|in.xfd>
//!   atari_xcheck make  <out.atr> FILE=path ...
//!
//! `dump` strips a 16-byte `.atr` header if present, opens the flat body
//! with the engine, and prints each file's name / size / sha256.

use std::fs;
use std::io::Cursor;

use rusty_backup::fs::atari_dos::AtariDosFilesystem;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}

/// Strip the 16-byte ATR header (magic 0x0296) if present.
fn flat_body(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() > 16 && bytes[0] == 0x96 && bytes[1] == 0x02 {
        bytes[16..].to_vec()
    } else {
        bytes
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("dump") => {
            let body = flat_body(fs::read(&args[2]).expect("read image"));
            let mut fs = AtariDosFilesystem::open(Cursor::new(body), 0).expect("open");
            println!("fs={}", fs.fs_type());
            let root = fs.root().unwrap();
            for e in fs.list_directory(&root).unwrap() {
                let data = fs.read_file(&e, usize::MAX).unwrap();
                println!(
                    "FILE name={:?} bytes={} sha256={}",
                    e.name,
                    data.len(),
                    sha256_hex(&data)
                );
            }
        }
        Some("make") => {
            // Build a blank SD image via the engine's create path, then add
            // files. (Reuses the test-only blank builder through a fresh
            // 92160-byte SD body the engine formats on the fly is not
            // exposed, so we ship a minimal hand-built blank here.)
            let body = blank_sd();
            let mut fs = AtariDosFilesystem::open(Cursor::new(body), 0).expect("open blank");
            let root = fs.root().unwrap();
            for spec in &args[3..] {
                let (atari_name, host) = spec.split_once('=').expect("NAME=path");
                let data = fs::read(host).expect("read host file");
                let mut cur = Cursor::new(data.clone());
                fs.create_file(
                    &root,
                    atari_name,
                    &mut cur,
                    data.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create_file");
                println!("added {atari_name} ({} bytes)", data.len());
            }
            let body = fs.into_inner().into_inner();
            // Wrap in a 16-byte ATR header so atrcopy / emulators accept it.
            let mut atr = vec![0u8; 16];
            atr[0] = 0x96;
            atr[1] = 0x02;
            let paras = (body.len() / 16) as u32;
            atr[2] = (paras & 0xFF) as u8;
            atr[3] = ((paras >> 8) & 0xFF) as u8;
            atr[4] = 128;
            atr[5] = 0;
            atr[6] = ((paras >> 16) & 0xFF) as u8;
            atr.extend_from_slice(&body);
            fs::write(&args[2], &atr).expect("write out");
            println!("wrote {} ({} bytes)", args[2], atr.len());
        }
        _ => {
            eprintln!("usage: atari_xcheck dump <in> | make <out.atr> NAME=path ...");
            std::process::exit(2);
        }
    }
}

/// Minimal blank single-density DOS 2.0S body (mirrors the engine's test
/// builder; kept here so the example is self-contained).
fn blank_sd() -> Vec<u8> {
    const SECTOR: usize = 128;
    const TOTAL: usize = 720;
    let mut img = vec![0u8; TOTAL * SECTOR];
    let voff = (360 - 1) * SECTOR;
    img[voff] = 2;
    let total = 707u16.to_le_bytes();
    img[voff + 1] = total[0];
    img[voff + 2] = total[1];
    img[voff + 3] = total[0];
    img[voff + 4] = total[1];
    for b in &mut img[voff + 0x0A..voff + 0x0A + 90] {
        *b = 0xFF;
    }
    let reserve = |img: &mut [u8], sector: usize| {
        let byte = voff + 0x0A + sector / 8;
        let bit = 7 - sector % 8;
        img[byte] &= !(1 << bit);
    };
    reserve(&mut img, 0);
    for s in 1..=3 {
        reserve(&mut img, s);
    }
    reserve(&mut img, 360);
    for s in 361..=368 {
        reserve(&mut img, s);
    }
    reserve(&mut img, 720);
    img
}
