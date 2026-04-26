use rusty_backup::fs::hfs::create_blank_hfs;
fn main() {
    let img = create_blank_hfs(2046 * 1024 * 1024, 32 * 1024, "Test").unwrap();
    print!("Primary MDB byte 10: 0x{:02x}\n", img[1024 + 10]);
    print!("Primary MDB byte 73: 0x{:02x}\n", img[1024 + 73]);
    let alt_off = img.len() - 1024;
    print!("Alt MDB byte 10: 0x{:02x}\n", img[alt_off + 10]);
    print!("Alt MDB byte 73: 0x{:02x}\n", img[alt_off + 73]);
}
