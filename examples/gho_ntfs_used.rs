//! Sanity-check the synthesized $Bitmap: open the volume as NTFS, read $Bitmap
//! through the real filesystem layer, and report used vs total. A correct
//! bitmap yields a plausible used figure (not 0, not ~total).
//!
//! usage: gho_ntfs_used <path-to.gho>

use rusty_backup::fs::filesystem::Filesystem;

fn main() {
    env_logger::init();
    let path = std::env::args()
        .nth(1)
        .expect("usage: gho_ntfs_used <path>");
    let reader =
        rusty_backup::rbformats::gho::GhoReader::open(std::path::Path::new(&path)).unwrap();
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(reader, 0).unwrap();
    fs.ensure_used_bytes();
    let total = fs.total_size();
    let used = fs.used_size();
    let gb = |b: u64| b as f64 / 1e9;
    println!("total = {:.2} GB", gb(total));
    println!(
        "used  = {:.2} GB ({:.1}%)",
        gb(used),
        used as f64 / total as f64 * 100.0
    );
    println!("free  = {:.2} GB", gb(total - used));
}
