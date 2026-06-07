//! Decode an EDSK file to flat sectors and write to a `.flat` sidecar.
//! Used to cross-check our EDSK decoder vs cpmtools' libdsk view.

use rusty_backup::rbformats::containers::edsk::decode_edsk_bytes;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: edsk_flat <file.dsk> [...]");
        std::process::exit(1);
    }
    for path in &args {
        let raw = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("read {path}: {e}");
                continue;
            }
        };
        let flat = match decode_edsk_bytes(&raw) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("decode {path}: {e}");
                continue;
            }
        };
        let out_path = format!("{path}.flat");
        if let Err(e) = std::fs::write(&out_path, &flat) {
            eprintln!("write {out_path}: {e}");
            continue;
        }
        eprintln!("wrote {} ({} bytes)", out_path, flat.len());
    }
}
