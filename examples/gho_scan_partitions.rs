//! Scan a directory tree of GHO/GHS files and print image_type +
//! partition_count for each PRIMARY file (skip .GHS spans).

use rusty_backup::rbformats::gho::{
    discover_gho_span_set, parse_gho_image, GhoContainerHeader, GhoImageType, SpanReader,
};
use std::fs::File;
use std::path::Path;

fn is_primary(p: &Path) -> bool {
    let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let lower = n.to_ascii_lowercase();
    if lower.ends_with(".gho") {
        return true;
    }
    if let Some((_, ext)) = lower.rsplit_once('.') {
        if ext.len() == 3 && ext.chars().all(|c| c.is_ascii_digit()) {
            // numeric span (e.g. .001 / .066); only the .001 is primary
            return ext == "001";
        }
    }
    false
}

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for ent in rd.flatten() {
        let p = ent.path();
        if p.is_dir() {
            walk(&p, out);
        } else if is_primary(&p) {
            out.push(p);
        }
    }
}

fn main() {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| format!("{}/new-fixtures/gho", std::env::var("HOME").unwrap()));
    let mut files = Vec::new();
    walk(Path::new(&root), &mut files);
    files.sort();
    println!(
        "{:<60} {:<12} {:>3} {:>5} {:<6}",
        "path", "image_type", "#p", "spans", "comp"
    );
    println!("{}", "-".repeat(100));
    for path in &files {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                println!("{} (open error: {e})", path.display());
                continue;
            }
        };
        let header = match GhoContainerHeader::parse(&mut f) {
            Ok(h) => h,
            Err(e) => {
                println!("{} (header parse error: {e})", path.display());
                continue;
            }
        };
        drop(f);
        // Use the SpanReader so partition_count covers boot-sector
        // records that live in span files, not just the primary.
        let span_set = match discover_gho_span_set(path) {
            Ok(s) => s,
            Err(_) => vec![path.clone()],
        };
        let (pc, spans) = match SpanReader::open(&span_set) {
            Ok(mut reader) => {
                use rusty_backup::rbformats::gho::DataLen as _;
                let total = reader.total_len();
                match parse_gho_image(&mut reader, total, &header) {
                    Ok(img) => (img.partition_count, span_set.len()),
                    Err(_) => (0, span_set.len()),
                }
            }
            Err(_) => (0, 1),
        };
        let kind = match header.image_type {
            GhoImageType::Sector => "Sector",
            GhoImageType::FileAware => "FileAware",
            GhoImageType::Other(b) => {
                println!(
                    "{:<60} other({:#04x}) {:>3} {:>5} {:?}",
                    path.display(),
                    b,
                    pc,
                    spans,
                    header.compression
                );
                continue;
            }
        };
        let pwd = if header.password_protected {
            " PWD"
        } else {
            ""
        };
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string();
        println!(
            "{:<60} {:<12} {:>3} {:>5} {:?}{}",
            rel, kind, pc, spans, header.compression, pwd
        );
    }
}
