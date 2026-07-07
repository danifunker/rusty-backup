//! Throwaway repro: mirror the GUI inspect-tab worker analysis on a disk
//! image, including the expensive ext minimum-size walk that the "Calc min"
//! button triggers. Run in debug (overflow checks on) to surface the panic.
//!
//!   cargo run --example repro_shork -- /Users/dani/Downloads/shork-486.vhd

use std::io::BufReader;

use rusty_backup::fs;
use rusty_backup::partition::{detect_alignment, PartitionTable};

fn main() {
    let path = std::env::args().nth(1).expect("usage: repro_shork <image>");
    println!("opening {path}");

    // Mirror the GUI inspect worker's open path for non-container files:
    // detect_image_format_with_path + wrap_image_reader (NOT open_read).
    println!("== detect_image_format_with_path (GUI path) ==");
    let f = std::fs::File::open(&path).unwrap();
    let format = rusty_backup::rbformats::detect_image_format_with_path(
        f,
        Some(std::path::Path::new(&path)),
    )
    .expect("detect_image_format");
    println!("  format = {}", format.description());

    println!("== wrap_image_reader ==");
    let f2 = std::fs::File::open(&path).unwrap();
    let (mut reader, data_size) =
        rusty_backup::rbformats::wrap_image_reader(f2, format).expect("wrap_image_reader");
    println!("  wrapped data_size = {data_size}");

    println!("== PartitionTable::detect ==");
    let table = PartitionTable::detect(&mut reader).expect("detect");
    println!("table ok");
    let _ = BufReader::new(std::fs::File::open(&path).unwrap()); // keep import

    println!("== detect_alignment ==");
    let alignment = detect_alignment(&table);
    println!("alignment = {:?}", alignment.alignment_type);

    let partitions = table.partitions();
    for part in &partitions {
        println!(
            "== partition {} type=0x{:02X} offset={} size={} ==",
            part.index,
            part.partition_type_byte,
            part.byte_offset(),
            part.size_bytes
        );

        println!("  probe_0x83_fs_type:");
        let mut r = rusty_backup::model::source_reader::open_read(std::path::Path::new(&path))
            .expect("reopen");
        let fam = fs::probe_0x83_fs_type(&mut r, part.byte_offset());
        println!("    -> {fam:?}");

        for (label, expensive) in [("deferred(false)", false), ("calc-min(true)", true)] {
            println!("  partition_minimum_size {label}:");
            let r = rusty_backup::model::source_reader::open_read(std::path::Path::new(&path))
                .expect("reopen");
            let res = fs::partition_minimum_size(
                r,
                part.byte_offset(),
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
                part.size_bytes,
                expensive,
                None,
                &|m| println!("      [progress] {m}"),
            );
            match res {
                fs::MinimumResult::Computed {
                    in_place,
                    defragmented,
                    fragmentation_percent,
                } => println!(
                    "    -> Computed in_place={in_place:?} defrag={defragmented:?} frag={fragmentation_percent:?}"
                ),
                fs::MinimumResult::Deferred { fs_name } => {
                    println!("    -> Deferred({fs_name})")
                }
            }
        }
    }

    println!("ALL DONE (no panic)");
}
