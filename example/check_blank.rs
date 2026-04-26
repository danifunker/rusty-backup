fn main() {
    let img = rusty_backup::fs::hfs::create_blank_hfs(2046 * 1024 * 1024, 32 * 1024, "Blank Test")
        .unwrap();
    println!("create_blank_hfs MDB byte 10: 0x{:02x}", img[1024 + 10]);
    println!("create_blank_hfs MDB byte 73: 0x{:02x}", img[1024 + 73]);
}
