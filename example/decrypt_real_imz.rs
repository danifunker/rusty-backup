//! Validate the WinImage IMZ decryption pipeline against the developer-local
//! known-plaintext sample:
//!   C:\temp\imz\w99_boot_withpassword.imz (password: "password")
//! Expect 1,474,560 bytes of output with CRC32 = 0xd4c4bba0.
//!
//! Not a test — the sample isn't in the repo. Run with:
//!   cargo run --example decrypt_real_imz

use std::io::Read;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from(r"C:\temp\imz\w99_boot_withpassword.imz");
    if !path.exists() {
        eprintln!("sample not found at {}; skipping", path.display());
        return;
    }
    let mut r =
        rusty_backup::rbformats::imz::ImzReader::open_with_password(&path, Some(b"password"))
            .expect("open with password");
    println!(
        "entry name = {:?}, logical_size = {}",
        r.entry_name(),
        r.logical_size()
    );
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).expect("read_to_end");
    let crc = crc32fast::hash(&buf);
    println!(
        "decrypted+inflated len = {}, crc32 = 0x{:08x}",
        buf.len(),
        crc
    );
    assert_eq!(buf.len(), 1_474_560, "length");
    assert_eq!(crc, 0xd4c4bba0, "CRC32 mismatch");
    println!("OK -- WinImage decrypt pipeline matches the known-plaintext oracle.");
}
