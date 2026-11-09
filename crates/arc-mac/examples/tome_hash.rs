//! Decode every fork of the given Installer Tomes and print one TSV line per non-empty fork:
//! file, record index, fork, length, compressed length, bytes consumed, SHA-1 (or the error).
//!
//! `cargo run --release -p arc-mac --example tome_hash -- Tome1 Tome2 ...`

use arc_mac::tome;
use sha1::{Digest, Sha1};

fn main() {
    let mut ok = 0usize;
    let mut bad = 0usize;
    for path in std::env::args().skip(1) {
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let name = std::path::Path::new(&path)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let entries = match tome::parse(&bytes) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("{path}: {e}");
                bad += 1;
                continue;
            }
        };
        for e in &entries {
            for (label, fork) in [("data", &e.data), ("rsrc", &e.rsrc)] {
                if fork.is_empty() {
                    continue;
                }
                let result = match tome::decode_fork(&bytes, fork) {
                    Ok(d) => {
                        if d.consumed == fork.compressed_len as usize {
                            ok += 1;
                        } else {
                            bad += 1;
                        }
                        let hex: String = Sha1::digest(&d.data)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        format!("{}\t{hex}", d.consumed)
                    }
                    Err(err) => {
                        bad += 1;
                        format!("-\tERR {err}")
                    }
                };
                println!(
                    "{name}\t{}\t{label}\t{}\t{}\t{result}",
                    e.index, fork.len, fork.compressed_len
                );
            }
        }
    }
    eprintln!("forks ok {ok}, bad {bad}");
}
