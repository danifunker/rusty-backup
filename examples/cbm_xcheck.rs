//! CBM DOS cross-validation tool: bridge between the rusty-backup `cbm`
//! engine and an external reference tool (the Python `d64` library /
//! VICE `c1541`).
//!
//! Usage:
//!   cbm_xcheck make  <out.d64> <variant> NAME ID  FILE=path[,TYPE] ...
//!   cbm_xcheck dump  <in.d64>
//!
//! `variant` is one of d64 / d64_40 / d71 / d81.
//! `dump` prints the disk name + each file's name / type / byte length /
//! sha256, so a reference tool's read can be diffed against ours.

use std::fs;
use std::io::Cursor;

use rusty_backup::fs::cbm::{create_blank, CbmFilesystem, CbmVariant};
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

fn variant_from(s: &str) -> CbmVariant {
    match s.to_ascii_lowercase().as_str() {
        "d64" => CbmVariant::D64,
        "d64_40" => CbmVariant::D64_40,
        "d71" => CbmVariant::D71,
        "d81" => CbmVariant::D81,
        "d80" => CbmVariant::D80,
        "d82" => CbmVariant::D82,
        other => panic!("unknown variant {other}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("make") => {
            let out = &args[2];
            let variant = variant_from(&args[3]);
            let name = &args[4];
            let id = &args[5];
            let img = create_blank(variant, name, id).expect("create_blank");
            let mut fs = CbmFilesystem::open(Cursor::new(img), 0).expect("open blank");
            let root = fs.root().unwrap();
            for spec in &args[6..] {
                // FILE=path[,TYPE]
                let (left, ftype) = match spec.split_once(',') {
                    Some((l, t)) => (l, Some(t.to_string())),
                    None => (spec.as_str(), None),
                };
                let (cbm_name, host_path) = left.split_once('=').expect("FILE=path");
                let data = fs::read(host_path).expect("read host file");
                let opts = CreateFileOptions {
                    type_code: ftype,
                    ..Default::default()
                };
                let mut cur = Cursor::new(data.clone());
                fs.create_file(&root, cbm_name, &mut cur, data.len() as u64, &opts)
                    .expect("create_file");
                println!("added {cbm_name} ({} bytes)", data.len());
            }
            let bytes = fs.into_inner().into_inner();
            fs::write(out, &bytes).expect("write out");
            println!("wrote {} ({} bytes)", out, bytes.len());
        }
        Some("dump") => {
            let path = &args[2];
            let bytes = fs::read(path).expect("read image");
            let mut fs = CbmFilesystem::open(Cursor::new(bytes), 0).expect("open");
            println!("disk_name={:?} fs={}", fs.volume_label(), fs.fs_type());
            let root = fs.root().unwrap();
            let entries = fs.list_directory(&root).unwrap();
            for e in &entries {
                let data = fs.read_file(e, usize::MAX).unwrap();
                println!(
                    "FILE name={:?} type={:?} bytes={} sha256={}",
                    e.name,
                    e.special_type.as_deref().unwrap_or("?"),
                    data.len(),
                    sha256_hex(&data)
                );
            }
        }
        _ => {
            eprintln!("usage: cbm_xcheck make <out> <variant> NAME ID FILE=path[,TYPE] ...");
            eprintln!("       cbm_xcheck dump <in>");
            std::process::exit(2);
        }
    }
}
