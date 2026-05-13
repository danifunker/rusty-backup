use rusty_backup::rbformats::chd::ChdReader;
use std::io::Read;
use std::path::PathBuf;
fn main() {
    let path = std::env::args().nth(1).expect("path");
    let mut r = ChdReader::open(&PathBuf::from(&path)).unwrap();
    let mut buf = [0u8; 512];
    r.read_exact(&mut buf).unwrap();
    println!(
        "First 4 (magic): {:02x?} = {:?}",
        &buf[..4],
        std::str::from_utf8(&buf[..4])
    );
    println!("First 16: {:02x?}", &buf[..16]);
    println!("Last 2 (boot sig): {:02x?}", &buf[510..512]);
}
