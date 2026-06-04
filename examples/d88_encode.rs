//! CLI: wrap a flat sector stream in a `.d88` container.
//!
//! Usage: d88_encode <flat.img> <out.d88> <cyls> <heads> <secs_per_track> <sec_size_bytes> [media]
//! where media is one of `2d`, `2dd`, `2hd` (default `2hd`).
//!
//! Used by `scripts/generate-d88-fixture.sh` to synthesize a fixture
//! from a flat FAT12 image.

use rusty_backup::rbformats::containers::d88::{encode_d88_bytes, D88Media};

fn parse_media(s: &str) -> Option<D88Media> {
    match s.to_ascii_lowercase().as_str() {
        "2d" => Some(D88Media::Dd2),
        "2dd" => Some(D88Media::Dd2dd),
        "2hd" => Some(D88Media::Dd2hd),
        _ => None,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!(
            "usage: d88_encode <flat.img> <out.d88> <cyls> <heads> <secs_per_track> <sec_size> [media:2d|2dd|2hd]"
        );
        std::process::exit(2);
    }
    let flat_path = &args[1];
    let out_path = &args[2];
    let cyls: u8 = args[3].parse().expect("cyls u8");
    let heads: u8 = args[4].parse().expect("heads u8");
    let spt: u8 = args[5].parse().expect("secs_per_track u8");
    let sec_size: usize = args[6].parse().expect("sec_size usize");
    let media = args
        .get(7)
        .and_then(|s| parse_media(s))
        .unwrap_or(D88Media::Dd2hd);
    let flat = std::fs::read(flat_path).expect("read flat");
    let encoded = encode_d88_bytes(&flat, cyls, heads, spt, sec_size, media).expect("encode d88");
    std::fs::write(out_path, &encoded).expect("write d88");
    eprintln!(
        "wrote {} ({} bytes) from {} ({} bytes flat, {}cyl × {}h × {}s × {}B {:?})",
        out_path,
        encoded.len(),
        flat_path,
        flat.len(),
        cyls,
        heads,
        spt,
        sec_size,
        media,
    );
}
