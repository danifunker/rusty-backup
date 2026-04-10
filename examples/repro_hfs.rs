use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Seek, SeekFrom};

fn main() {
    std::fs::copy(
        "/Users/dani/Downloads/EmptyHFS.hda",
        "/tmp/EmptyHFS_copy.hda",
    )
    .unwrap();

    // First read catalog extents from MDB manually
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/tmp/EmptyHFS_copy.hda")
        .unwrap();
    f.seek(SeekFrom::Start(0xC000 + 1024)).unwrap();
    let mut mdb = [0u8; 512];
    f.read_exact(&mut mdb).unwrap();
    let block_size = BigEndian::read_u32(&mdb[20..24]);
    let first_alloc_block = BigEndian::read_u16(&mdb[28..30]) as u64;
    let catalog_file_size = BigEndian::read_u32(&mdb[146..150]);
    let cat_start = BigEndian::read_u16(&mdb[150..152]) as u64;
    let cat_blocks = BigEndian::read_u16(&mdb[152..154]) as u64;
    let total_blocks = BigEndian::read_u16(&mdb[18..20]);
    let free_blocks = BigEndian::read_u16(&mdb[34..36]);
    let vbm_block = BigEndian::read_u16(&mdb[14..16]);
    let ext_size = BigEndian::read_u32(&mdb[130..134]);
    let ext_start = BigEndian::read_u16(&mdb[134..136]);
    let ext_count = BigEndian::read_u16(&mdb[136..138]);
    println!("BEFORE:");
    println!("  block_size={block_size} first_alloc_block_sector={first_alloc_block} total_blocks={total_blocks} free_blocks={free_blocks} vbm_sector={vbm_block}");
    println!("  catalog: size={catalog_file_size} ext0: start={cat_start} blocks={cat_blocks}");
    println!("  extents: size={ext_size} ext0: start={ext_start} blocks={ext_count}");

    let cat_offset = 0xC000u64 + first_alloc_block * 512 + cat_start * block_size as u64;
    f.seek(SeekFrom::Start(cat_offset)).unwrap();
    let mut cat = vec![0u8; catalog_file_size as usize];
    f.read_exact(&mut cat).unwrap();
    println!(
        "  catalog[32..34] (node_size) = {:02x?} → {}",
        &cat[32..34],
        BigEndian::read_u16(&cat[32..34])
    );
    drop(f);

    // Open as HfsFilesystem and attempt create_file.
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/tmp/EmptyHFS_copy.hda")
        .unwrap();
    let mut fs = HfsFilesystem::open(f, 0xC000).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    println!("  entries before create: {}", entries.len());

    let data = b"Hello from rusty-backup!";
    let mut cursor = Cursor::new(data.as_slice());
    let opts = CreateFileOptions::default();
    match fs.create_file(&root, "Hello.txt", &mut cursor, data.len() as u64, &opts) {
        Ok(fe) => println!("  created: {} (cnid={})", fe.name, fe.location),
        Err(e) => {
            eprintln!("  create_file failed: {e}");
            return;
        }
    }
    fs.sync_metadata().unwrap();
    drop(fs);

    // Re-open and verify
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/tmp/EmptyHFS_copy.hda")
        .unwrap();
    let mut fs = HfsFilesystem::open(f, 0xC000).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    println!("AFTER reopen:");
    println!("  entries: {}", entries.len());
    for e in &entries {
        println!("    {} (size={}, loc={})", e.name, e.size, e.location);
    }

    // Run fsck to sanity-check the initialized volume
    match fs.fsck() {
        Ok(result) => {
            println!(
                "  fsck: {} errors, {} warnings",
                result.errors.len(),
                result.warnings.len()
            );
            for issue in result.errors.iter().chain(result.warnings.iter()) {
                println!("    [{}] {}", issue.code, issue.message);
            }
        }
        Err(e) => eprintln!("  fsck failed: {e}"),
    }
}
