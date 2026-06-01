//! Run prepare_full_image() and dump the $Boot region the reader serves.
use std::io::{Read, Seek, SeekFrom};

fn main() {
    env_logger::init();
    let path = std::env::args().nth(1).unwrap();
    let mut r = rusty_backup::rbformats::gho::GhoReader::open(std::path::Path::new(&path)).unwrap();
    r.prepare_full_image();
    r.seek(SeekFrom::Start(0)).unwrap();
    let mut b = vec![0u8; 8192];
    let mut f = 0;
    while f < b.len() {
        match r.read(&mut b[f..]).unwrap() {
            0 => break,
            k => f += k,
        }
    }
    println!("sector0[0..16]:  {:02x?}", &b[0..16]);
    println!("sector1[0..16]:  {:02x?}", &b[512..512 + 16]);
    let s: String = b[512..]
        .iter()
        .map(|&c| {
            if c.is_ascii_graphic() || c == b' ' {
                c as char
            } else {
                '.'
            }
        })
        .collect();
    let ascii: String = s.chars().filter(|c| *c != '.').collect();
    println!(
        "ascii in 512..8192 (compacted): {}",
        &ascii[..ascii.len().min(300)]
    );
}
