// Dump the "sentinel" runs — stream runs the lazy scan recorded but couldn't
// assign an LCN. Their decoded first bytes reveal what the recoverable-but-
// unplaced data is (regf/hbin = registry hive, INDX/FILE = metadata, else
// opaque). Tells us whether the SYSTEM hive's missing clusters are here.
//
// Usage: cargo run --release --example gho_dump_sentinels -- PATH.GHO

use rusty_backup::rbformats::gho::GhoReader;

fn classify(b: &[u8]) -> String {
    if b.len() < 4 {
        return "short".into();
    }
    match &b[0..4] {
        m if m == b"regf" => "regf (registry hive base block)".into(),
        m if m == b"hbin" => "hbin (registry hive bin)".into(),
        m if m == b"INDX" => "INDX".into(),
        m if m == b"FILE" => "FILE".into(),
        m if m == b"RSTR" || m == b"RCRD" => "LogFile".into(),
        _ => {
            if b.iter().all(|&x| x == 0) {
                "zeros".into()
            } else {
                format!(
                    "data[{}]",
                    b.iter()
                        .take(8)
                        .map(|x| format!("{x:02x}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).expect("path to GHO");
    let mut r = GhoReader::open(std::path::Path::new(&path))?;
    r.prepare_full_image();
    let sentinels = r.debug_sentinel_runs_full(64);
    println!("sentinel runs: {}", sentinels.len());
    let total: u64 = sentinels.iter().map(|(cc, _, _)| cc).sum();
    println!("total sentinel clusters: {total}");
    println!();
    for (i, (cc, seq, bytes)) in sentinels.iter().enumerate() {
        println!(
            "  #{i:<3} cc={cc:<6} seq={seq:<4} content={}",
            classify(bytes)
        );
    }
    Ok(())
}
