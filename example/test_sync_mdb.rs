// Open a fresh blank HFS image, call sync_metadata, check primary vs alt MDB byte 10.
use rusty_backup::fs::filesystem::EditableFilesystem;
use rusty_backup::fs::hfs::{create_blank_hfs, HfsFilesystem};
use std::io::Cursor;

fn main() {
    let mut img = create_blank_hfs(2046 * 1024 * 1024, 32 * 1024, "Test").unwrap();
    let img_len = img.len();
    println!(
        "Initial: primary[10]=0x{:02x} alt[10]=0x{:02x}",
        img[1024 + 10],
        img[img_len - 1024 + 10]
    );

    {
        let cursor = Cursor::new(&mut img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();
        fs.sync_metadata().unwrap();
    }
    let img_len = img.len();
    println!(
        "After sync: primary[10]=0x{:02x} alt[10]=0x{:02x}",
        img[1024 + 10],
        img[img_len - 1024 + 10]
    );
    println!(
        "           primary[73]=0x{:02x} alt[73]=0x{:02x}",
        img[1024 + 73],
        img[img_len - 1024 + 73]
    );

    // First 162 bytes diff
    let primary = &img[1024..1024 + 162];
    let alt = &img[img_len - 1024..img_len - 1024 + 162];
    let diffs: Vec<_> = (0..162).filter(|&i| primary[i] != alt[i]).collect();
    println!("Diff positions: {:?}", diffs);
    for i in &diffs {
        println!(
            "  byte {}: primary=0x{:02x} alt=0x{:02x}",
            i, primary[*i], alt[*i]
        );
    }
}
