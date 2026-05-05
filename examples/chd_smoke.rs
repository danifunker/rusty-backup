use libchdman_rs::Chd;
use std::env;

fn main() {
    let path = env::args().nth(1).expect("usage: chd_smoke <path-to-chd>");
    let chd = Chd::open(&path, false, None).expect("failed to open CHD");
    let info = chd.info().expect("failed to read info");
    println!("path:          {path}");
    println!("version:       {}", info.version);
    println!("logical_bytes: {}", info.logical_bytes);
    println!("hunk_bytes:    {}", info.hunk_bytes);
    println!("unit_bytes:    {}", info.unit_bytes);
    println!("hunk_count:    {}", info.hunk_count);
    println!(
        "codecs:        [{:08x}, {:08x}, {:08x}, {:08x}]",
        info.codecs[0], info.codecs[1], info.codecs[2], info.codecs[3]
    );
    print!("sha1:          ");
    for b in info.sha1 {
        print!("{b:02x}");
    }
    println!();
    println!(
        "flags:         hd={} cd={} gd={} dvd={} av={}",
        info.is_hd, info.is_cd, info.is_gd, info.is_dvd, info.is_av
    );
    println!("track_count:   {}", info.track_count);
}
