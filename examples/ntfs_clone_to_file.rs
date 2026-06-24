//! Build a populated NTFS source in memory, stream a defragmenting clone to a
//! file, for oracle validation. Usage: cargo run --example ntfs_clone_to_file -- <out>
use rusty_backup::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use rusty_backup::fs::ntfs::NtfsFilesystem;
use rusty_backup::fs::ntfs_clone::stream_defragmented_ntfs;
use rusty_backup::fs::ntfs_format::{create_blank_ntfs, ntfs_min_packed_size};
use std::io::{Cursor, Seek, SeekFrom};

fn main() {
    let out = std::env::args().nth(1).unwrap();
    let mut cur = Cursor::new(Vec::<u8>::new());
    create_blank_ntfs(&mut cur, 48 * 1024 * 1024, 128, Some("CLONESRC")).unwrap();
    cur.seek(SeekFrom::Start(0)).unwrap();
    let mut src = NtfsFilesystem::open(cur, 0).unwrap();
    let root = src.root().unwrap();
    let w = |fs: &mut NtfsFilesystem<Cursor<Vec<u8>>>, p: &_, n: &str, b: &[u8]| {
        let mut c = Cursor::new(b.to_vec());
        fs.create_file(p, n, &mut c, b.len() as u64, &CreateFileOptions::default())
            .unwrap();
    };
    w(&mut src, &root, "hello.txt", b"cloned by rusty-backup\n");
    w(&mut src, &root, "big.bin", &vec![0xA5u8; 50_000]);
    let sub = src
        .create_directory(&root, "docs", &CreateDirectoryOptions::default())
        .unwrap();
    w(&mut src, &sub, "note.txt", b"nested file\n");
    EditableFilesystem::sync_metadata(&mut src).unwrap();

    let target_size = ntfs_min_packed_size(src.used_size(), 64, 4096);
    let mut f = std::fs::File::create(&out).unwrap();
    let r =
        stream_defragmented_ntfs(&mut src, target_size, &mut f, &mut |_| {}, &mut |_| {}).unwrap();
    println!(
        "cloned -> {out} ({} MiB): {} files / {} dirs",
        target_size / 1048576,
        r.files_copied,
        r.dirs_copied
    );
}
