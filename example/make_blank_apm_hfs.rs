// Usage: make_blank_apm_hfs <source.hda> <out.hda> [size_mib] [block_kib]
// Builds a blank HFS volume of the requested size + block size, wraps in APM
// using drivers from `source.hda`, writes to `out.hda`.
use rusty_backup::fs::hfs::create_blank_hfs;
use rusty_backup::fs::hfs_clone::emit_apm_disk_with_hfs;
use std::fs::{File, OpenOptions};
use std::io::Seek;

fn main() {
    let mut args = std::env::args().skip(1);
    let src = args.next().expect("source.hda");
    let out = args.next().expect("out.hda");
    let size_mib: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(2046);
    let bs_kib: u32 = args.next().map(|s| s.parse().unwrap()).unwrap_or(32);

    let target_size = size_mib * 1024 * 1024;
    let block_size = bs_kib * 1024;
    println!(
        "Creating blank HFS: {} MiB, {} KiB blocks",
        size_mib, bs_kib
    );
    let img = create_blank_hfs(target_size, block_size, "Blank Test").unwrap();
    println!(
        "HFS image: {} bytes ({} sectors)",
        img.len(),
        img.len() / 512
    );

    let mut src_f = File::open(&src).unwrap();
    let src_size = src_f.seek(std::io::SeekFrom::End(0)).unwrap();
    let mut out_f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&out)
        .unwrap();
    let report = emit_apm_disk_with_hfs(&mut src_f, src_size, &img, &mut out_f).unwrap();
    println!(
        "Emit done: hfs_start={} hfs_block_count={}",
        report.hfs_start_block, report.hfs_block_count
    );
}
